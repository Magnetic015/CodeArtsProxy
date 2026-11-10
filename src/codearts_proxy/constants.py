BASE_HEADERS = {
    "Accept": "application/json",
    "Referer": "/chat/home",
    "x-requested-with": "XMLHttpRequest",
    "x-codearts-doer-scenario-type": "web",
    "x-language": "zh-cn",
    "x-auth-token": "",
}

SESSION_CREATE = "/chat/v1/cloudagent/sessions"
SESSION_MESSAGES = "/chat/codebaseservice/v1/cloudagent/sessions/{session_id}/messages"
SESSION_STOP = "/chat/v1/cloudagent/sessions/{session_id}/messages/stop"
MODELS = "/chat/PromptCenterService/v1/model/builtin"
IDENTITY = "/chat/rest/me"
ATTACHMENTS = "/chat/codebaseservice/v1/cloudagent/attachments"
OPS_BASE = "/chat/PromptCenterService/v1/ops"

TERMINAL_STREAM_ERRORS = {"stream_ended", "CODEBASE.00010628.200"}
