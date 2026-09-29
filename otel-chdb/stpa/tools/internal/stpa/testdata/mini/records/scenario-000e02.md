---
id: scenario-000e02
label: LS-1
title: The store answered late; the writer believed the batch was not committed and wrote it again
feedback: [writer->store/feedback/status]
factor: feedback-late
hazards: [hazard-000a02]
variables: [writer/committed]
formerly: [UCA-2]
state: accepted
---
