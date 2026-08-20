# @source-inc/gents-desktop-operations

Focused operator surfaces for tool-call holds, backend/MCP health, request traces,
and workspace inspection. Tool lifecycle, background work, and subagent progress
belong to the conversation timeline in `@source-inc/gents-desktop-chat`.

```ts
import {
  HoldsPanel,
  RequestTracePanel,
} from "@source-inc/gents-desktop-operations";
```

```css
@import "@source-inc/gents-desktop-tokens/semantic.css";
@import "@source-inc/gents-desktop-ui/styles.css";
@import "@source-inc/gents-desktop-operations/styles.css";
```

Standalone panels accept an `api` prop or can be wrapped with
`OperationsApiProvider`. Required grants depend on the selected panel: holds,
operations-read, trace-read, or workspace-read.
