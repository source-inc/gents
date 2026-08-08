Job {{ doc.job_id }} (arm {{ doc.arm }}, suite {{ doc.suite }}) fired by
trigger {{ event.trigger_id }} on {{ event.source_collection }} doc
{{ event.source_doc_id }} at {{ ctx.now }}, handled by behavior
{{ node.behavior_id }} on {{ node.node_did }}.

{{ doc.prompt }}

After answering, call the only available tool write_experiment_finding for
job_id={{ doc.job_id }} (unique finding_id, content, stage="stage1").
