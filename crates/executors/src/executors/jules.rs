use std::{
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
};

use async_trait::async_trait;
use derivative::Derivative;
use futures::StreamExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{io::AsyncWriteExt, process::Command};
use ts_rs::TS;
use workspace_utils::{msg_store::MsgStore, log_msg::LogMsg};

use command_group::AsyncCommandGroup;

use crate::{
    approvals::ExecutorApprovalService,
    command::CmdOverrides,
    env::ExecutionEnv,
    executors::{
        AppendPrompt, AvailabilityInfo, ExecutorError, SpawnedChild, StandardCodingAgentExecutor,
    },
    logs::{
        ActionType, FileChange, NormalizedEntry, NormalizedEntryType, ToolStatus,
        utils::{EntryIndexProvider, patch::ConversationPatch},
    },
    stdout_dup::create_stdout_pipe_writer,
};

#[derive(Derivative, Clone, Serialize, Deserialize, TS, JsonSchema)]
#[derivative(Debug, PartialEq)]
pub struct Jules {
    #[serde(default)]
    pub append_prompt: AppendPrompt,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub automation_mode: Option<String>, // e.g., "AUTO_CREATE_PR"
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub require_plan_approval: Option<bool>,
    #[serde(flatten)]
    pub cmd: CmdOverrides,
    #[serde(skip)]
    #[ts(skip)]
    #[derivative(Debug = "ignore", PartialEq = "ignore")]
    pub approvals: Option<Arc<dyn ExecutorApprovalService>>,
}

impl Jules {
    fn build_bridge_script(&self) -> String {
        const BRIDGE_SCRIPT: &str = r#"
import https from 'https';
import fs from 'fs';
import path from 'path';
import { execSync } from 'child_process';
import os from 'os';

// Get args
const prompt = process.argv[2];
const workDir = process.argv[3] || process.cwd();
const automationMode = process.env.JULES_AUTOMATION_MODE || 'AUTOMATION_MODE_UNSPECIFIED';
// Interactive plan approval is not yet supported in this bridge.
// We default to false to prevent hanging.
const requirePlanApproval = false;

const API_KEY = process.env.JULES_API_KEY;
if (!API_KEY) {
    console.error(JSON.stringify({ type: 'error', message: 'JULES_API_KEY is not set' }));
    process.exit(1);
}

function apiRequest(method, endpoint, body = null) {
    return new Promise((resolve, reject) => {
        const options = {
            hostname: 'jules.googleapis.com',
            path: '/v1alpha' + endpoint,
            method: method,
            headers: {
                'x-goog-api-key': API_KEY,
                'Content-Type': 'application/json',
            },
        };

        const req = https.request(options, (res) => {
            let data = '';
            res.on('data', (chunk) => (data += chunk));
            res.on('end', () => {
                if (res.statusCode >= 200 && res.statusCode < 300) {
                    try {
                        resolve(data ? JSON.parse(data) : {});
                    } catch (e) {
                        resolve({});
                    }
                } else {
                    reject(new Error(`API Error ${res.statusCode}: ${data}`));
                }
            });
        });

        req.on('error', (e) => reject(e));
        if (body) {
            req.write(JSON.stringify(body));
        }
        req.end();
    });
}

function getGitInfo() {
    try {
        const remoteUrl = execSync('git remote get-url origin', { cwd: workDir, encoding: 'utf8' }).trim();
        const currentBranch = execSync('git rev-parse --abbrev-ref HEAD', { cwd: workDir, encoding: 'utf8' }).trim();

        // Parse owner/repo from URL
        // Supports:
        // https://github.com/owner/repo.git
        // git@github.com:owner/repo.git
        // ssh://git@github.com/owner/repo.git
        const match = remoteUrl.match(/github\.com[:/]([^/]+)\/([^.]+)(\.git)?$/);
        if (match) {
            return { owner: match[1], repo: match[2], branch: currentBranch || 'main' };
        }
    } catch (e) {
        // ignore
    }
    return null;
}

async function findSource(owner, repo) {
    let pageToken = null;
    do {
        const query = pageToken ? `?pageToken=${pageToken}&pageSize=100` : '?pageSize=100';
        const response = await apiRequest('GET', `/sources${query}`);
        const sources = response.sources || [];

        for (const source of sources) {
            if (source.githubRepo &&
                source.githubRepo.owner === owner &&
                source.githubRepo.repo === repo) {
                return source.name;
            }
        }
        pageToken = response.nextPageToken;
    } while (pageToken);
    return null;
}

async function applyPatch(patchContent) {
    const tmpFile = path.join(os.tmpdir(), `jules_patch_${Date.now()}.diff`);
    try {
        fs.writeFileSync(tmpFile, patchContent);
        execSync(`git apply ${tmpFile}`, { cwd: workDir });
        fs.unlinkSync(tmpFile);
        return true;
    } catch (e) {
        console.error(JSON.stringify({ type: 'error', message: `Failed to apply patch: ${e.message}` }));
        try { fs.unlinkSync(tmpFile); } catch (e2) {}
        return false;
    }
}

async function main() {
    const gitInfo = getGitInfo();
    if (!gitInfo) {
        console.error(JSON.stringify({ type: 'error', message: 'Could not determine GitHub repository from current directory.' }));
        process.exit(1);
    }

    const sourceName = await findSource(gitInfo.owner, gitInfo.repo);
    if (!sourceName) {
        console.error(JSON.stringify({ type: 'error', message: `Could not find a connected Jules source for ${gitInfo.owner}/${gitInfo.repo}. Please connect it in the Jules dashboard.` }));
        process.exit(1);
    }

    // Create Session
    let session;
    try {
        session = await apiRequest('POST', '/sessions', {
            prompt: prompt,
            sourceContext: {
                source: sourceName,
                githubRepoContext: {
                    startingBranch: gitInfo.branch
                }
            },
            automationMode: automationMode,
            requirePlanApproval: requirePlanApproval
        });
        console.log(JSON.stringify({ type: 'session_created', session }));
    } catch (e) {
        console.error(JSON.stringify({ type: 'error', message: `Failed to create session: ${e.message}` }));
        process.exit(1);
    }

    const sessionId = session.id;

    // Poll for activities
    let processedActivities = new Set();
    let isDone = false;

    while (!isDone) {
        try {
            // Use session.name which is sessions/{id}
            const response = await apiRequest('GET', `/${session.name}/activities?pageSize=100`);
            const activities = response.activities || [];

            for (const activity of activities) {
                if (!processedActivities.has(activity.id)) {
                    processedActivities.add(activity.id);
                    console.log(JSON.stringify({ type: 'activity', activity }));

                    // Handle Artifacts (Patches)
                    if (activity.artifacts) {
                        for (const artifact of activity.artifacts) {
                            if (artifact.changeSet && artifact.changeSet.gitPatch) {
                                const unidiff = artifact.changeSet.gitPatch.unidiffPatch;
                                if (unidiff) {
                                    const applied = await applyPatch(unidiff);
                                    if (applied) {
                                        console.log(JSON.stringify({
                                            type: 'patch_applied',
                                            success: true,
                                            activityId: activity.id
                                        }));
                                    }
                                }
                            }
                        }
                    }

                    if (activity.sessionCompleted) {
                        isDone = true;
                    }
                    if (activity.sessionFailed) {
                        isDone = true;
                    }
                }
            }

            // Check session status just in case
            if (!isDone) {
                const sessionCheck = await apiRequest('GET', `/${session.name}`);
                if (sessionCheck.state === 'COMPLETED' || sessionCheck.state === 'FAILED') {
                     isDone = true;
                     // Log session output (e.g. PR link)
                     if (sessionCheck.outputs) {
                         console.log(JSON.stringify({ type: 'session_output', output: sessionCheck.outputs }));
                     }
                }
            }

        } catch (e) {
            console.error(JSON.stringify({ type: 'error', message: `Polling error: ${e.message}` }));
        }

        if (!isDone) {
            await new Promise(r => setTimeout(r, 2000));
        }
    }
}

main();
"#;
        BRIDGE_SCRIPT.to_string()
    }
}

#[async_trait]
impl StandardCodingAgentExecutor for Jules {
    fn use_approvals(&mut self, approvals: Arc<dyn ExecutorApprovalService>) {
        self.approvals = Some(approvals);
    }

    async fn spawn(
        &self,
        current_dir: &Path,
        prompt: &str,
        env: &ExecutionEnv,
    ) -> Result<SpawnedChild, ExecutorError> {
        // Write bridge script to a temp file
        let bridge_script_content = self.build_bridge_script();
        let mut temp_file = std::env::temp_dir();
        temp_file.push(format!("jules_bridge_{}.mjs", uuid::Uuid::new_v4()));

        {
            let mut file = tokio::fs::File::create(&temp_file).await.map_err(ExecutorError::Io)?;
            file.write_all(bridge_script_content.as_bytes()).await.map_err(ExecutorError::Io)?;
        }

        let combined_prompt = self.append_prompt.combine_prompt(prompt);

        let mut command = Command::new("node");
        command
            .arg(&temp_file)
            .arg(&combined_prompt)
            .arg(current_dir)
            .current_dir(current_dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        if let Some(mode) = &self.automation_mode {
            command.env("JULES_AUTOMATION_MODE", mode);
        }

        // Pass API key if available in env
        if let Ok(api_key) = std::env::var("JULES_API_KEY") {
            command.env("JULES_API_KEY", api_key);
        }

        env.clone()
            .with_profile(&self.cmd)
            .apply_to_command(&mut command);

        let mut child = command.group_spawn()?;

        // Ensure we take stdout so it doesn't leak or block
        let _child_stdout = child.inner().stdout.take().ok_or_else(|| {
            ExecutorError::Io(std::io::Error::other("Jules bridge missing stdout"))
        })?;
        let _child_stdin = child.inner().stdin.take();

        // Use stdout_dup to pipe output to both log processor and original stdout if needed.
        // For standard coding agents, we usually rely on `create_stdout_pipe_writer` to ensure
        // the `SpawnedChild` has the correct setup for reading logs.
        let _new_stdout = create_stdout_pipe_writer(&mut child)?;

        Ok(SpawnedChild {
            child,
            exit_signal: None,
            interrupt_sender: None,
        })
    }

    async fn spawn_follow_up(
        &self,
        _current_dir: &Path,
        _prompt: &str,
        _session_id: &str,
        _env: &ExecutionEnv,
    ) -> Result<SpawnedChild, ExecutorError> {
        Err(ExecutorError::FollowUpNotSupported("Jules does not support follow-up yet".to_string()))
    }

    fn normalize_logs(&self, msg_store: Arc<MsgStore>, _worktree_path: &Path) {
        JulesLogProcessor::process_logs(msg_store);
    }

    fn default_mcp_config_path(&self) -> Option<PathBuf> {
        None
    }

    fn get_availability_info(&self) -> AvailabilityInfo {
        if std::env::var("JULES_API_KEY").is_ok() {
             AvailabilityInfo::InstallationFound
        } else {
             AvailabilityInfo::NotFound
        }
    }
}

struct JulesLogProcessor;

impl JulesLogProcessor {
    pub fn process_logs(msg_store: Arc<MsgStore>) {
        let entry_index_provider = EntryIndexProvider::start_from(&msg_store);

        tokio::spawn(async move {
            let mut stream = msg_store.history_plus_stream();
            let mut buffer = String::new();

            while let Some(Ok(msg)) = stream.next().await {
                 let chunk = match msg {
                    LogMsg::Stdout(x) => x,
                    LogMsg::JsonPatch(_)
                    | LogMsg::SessionId(_)
                    | LogMsg::Stderr(_)
                    | LogMsg::Ready => continue,
                    LogMsg::Finished => break,
                };

                buffer.push_str(&chunk);

                // Process complete JSON lines
                for line in buffer
                    .split_inclusive('\n')
                    .filter(|l| l.ends_with('\n'))
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
                {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }

                    if let Ok(json) = serde_json::from_str::<Value>(trimmed) {
                        Self::handle_json(&json, &msg_store, &entry_index_provider);
                    }
                }

                buffer = buffer.rsplit('\n').next().unwrap_or("").to_owned();
            }
        });
    }

    fn handle_json(json: &Value, msg_store: &Arc<MsgStore>, entry_index_provider: &EntryIndexProvider) {
        let msg_type = json.get("type").and_then(|s| s.as_str());

        match msg_type {
            Some("error") => {
                 let message = json.get("message").and_then(|s| s.as_str()).unwrap_or("Unknown error");
                 let entry = NormalizedEntry {
                    timestamp: None,
                    entry_type: NormalizedEntryType::ErrorMessage { error_type: crate::logs::NormalizedEntryError::Other },
                    content: message.to_string(),
                    metadata: None,
                };
                let id = entry_index_provider.next();
                msg_store.push_patch(ConversationPatch::add_normalized_entry(id, entry));
            }
            Some("session_created") => {
                if let Some(session) = json.get("session") {
                     if let Some(session_id) = session.get("id").and_then(|s| s.as_str()) {
                         msg_store.push_session_id(session_id.to_string());
                     }
                     // Maybe log session creation?
                     let entry = NormalizedEntry {
                        timestamp: None,
                        entry_type: NormalizedEntryType::SystemMessage,
                        content: format!("Session created: {}", session.get("name").and_then(|s| s.as_str()).unwrap_or("unknown")),
                        metadata: Some(session.clone()),
                    };
                    let id = entry_index_provider.next();
                    msg_store.push_patch(ConversationPatch::add_normalized_entry(id, entry));
                }
            }
            Some("activity") => {
                if let Some(activity) = json.get("activity") {
                    Self::handle_activity(activity, msg_store, entry_index_provider);
                }
            }
            Some("patch_applied") => {
                // We handle the "apply" logic by logging it in handle_activity, but we can verify success here
            }
            Some("session_output") => {
                if let Some(outputs) = json.get("output").and_then(|o| o.as_array()) {
                    for output in outputs {
                        if let Some(pr) = output.get("pullRequest") {
                            let url = pr.get("url").and_then(|s| s.as_str()).unwrap_or("");
                            let entry = NormalizedEntry {
                                timestamp: None,
                                entry_type: NormalizedEntryType::SystemMessage,
                                content: format!("Pull Request Created: {}", url),
                                metadata: Some(output.clone()),
                            };
                            let id = entry_index_provider.next();
                            msg_store.push_patch(ConversationPatch::add_normalized_entry(id, entry));
                        }
                    }
                }
            }
            _ => {}
        }
    }

    fn handle_activity(activity: &Value, msg_store: &Arc<MsgStore>, entry_index_provider: &EntryIndexProvider) {
        // Check for specific activity types
        if let Some(plan_generated) = activity.get("planGenerated") {
            // Plan Generated
            let plan = plan_generated.get("plan").map(|p| p.to_string()).unwrap_or_default();
             let entry = NormalizedEntry {
                timestamp: None,
                entry_type: NormalizedEntryType::SystemMessage,
                content: format!("Plan generated: {}", plan), // TODO: Format plan nicely
                metadata: Some(activity.clone()),
            };
            let id = entry_index_provider.next();
            msg_store.push_patch(ConversationPatch::add_normalized_entry(id, entry));
        } else if let Some(agent_messaged) = activity.get("agentMessaged") {
            let message = agent_messaged.get("agentMessage").and_then(|s| s.as_str()).unwrap_or("");
            let entry = NormalizedEntry {
                timestamp: None,
                entry_type: NormalizedEntryType::AssistantMessage,
                content: message.to_string(),
                metadata: Some(activity.clone()),
            };
            let id = entry_index_provider.next();
            msg_store.push_patch(ConversationPatch::add_normalized_entry(id, entry));
        } else if let Some(user_messaged) = activity.get("userMessaged") {
            let message = user_messaged.get("userMessage").and_then(|s| s.as_str()).unwrap_or("");
            let entry = NormalizedEntry {
                timestamp: None,
                entry_type: NormalizedEntryType::UserMessage,
                content: message.to_string(),
                metadata: Some(activity.clone()),
            };
            let id = entry_index_provider.next();
            msg_store.push_patch(ConversationPatch::add_normalized_entry(id, entry));
        } else if let Some(artifacts) = activity.get("artifacts").and_then(|a| a.as_array()) {
             for artifact in artifacts {
                 if let Some(change_set) = artifact.get("changeSet") {
                     if let Some(git_patch) = change_set.get("gitPatch") {
                         let unidiff = git_patch.get("unidiffPatch").and_then(|s| s.as_str());
                         if let Some(diff) = unidiff {
                              let entry = NormalizedEntry {
                                timestamp: None,
                                entry_type: NormalizedEntryType::ToolUse {
                                    tool_name: "ApplyPatch".to_string(),
                                    action_type: ActionType::FileEdit {
                                        path: "Apply Patch".to_string(), // We don't have a single path for a patch usually
                                        changes: vec![FileChange::Edit {
                                            unified_diff: diff.to_string(),
                                            has_line_numbers: false
                                        }]
                                    },
                                    status: ToolStatus::Success
                                },
                                content: "Applying code changes...".to_string(),
                                metadata: Some(artifact.clone()),
                            };
                            let id = entry_index_provider.next();
                            msg_store.push_patch(ConversationPatch::add_normalized_entry(id, entry));
                         }
                     }
                 }
             }
        } else if activity.get("sessionCompleted").is_some() {
             let entry = NormalizedEntry {
                timestamp: None,
                entry_type: NormalizedEntryType::SystemMessage,
                content: "Session completed successfully.".to_string(),
                metadata: Some(activity.clone()),
            };
            let id = entry_index_provider.next();
            msg_store.push_patch(ConversationPatch::add_normalized_entry(id, entry));
        } else if let Some(failed) = activity.get("sessionFailed") {
            let reason = failed.get("reason").and_then(|s| s.as_str()).unwrap_or("Unknown reason");
             let entry = NormalizedEntry {
                timestamp: None,
                entry_type: NormalizedEntryType::ErrorMessage { error_type: crate::logs::NormalizedEntryError::Other },
                content: format!("Session failed: {}", reason),
                metadata: Some(activity.clone()),
            };
            let id = entry_index_provider.next();
            msg_store.push_patch(ConversationPatch::add_normalized_entry(id, entry));
        }
    }
}
