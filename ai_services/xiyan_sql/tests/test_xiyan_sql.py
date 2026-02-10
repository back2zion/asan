# This monolithic test file has been split into 4 focused modules:
#
#   test_prompt_extract.py  — TestBuildPrompt + TestExtractSQL
#   test_schema_evidence.py — TestSchema (OMOP CDM schema & evidence)
#   test_generate_sql.py    — TestGenerateSQL + TestConversationMemoryIntegration + TestRecordQueryResult
#   test_schema_linker.py   — TestSchemaLinker + TestDynamicSchemaIntegration + TestServiceSchemaLinkerIntegration
#
# Run all:  pytest ai_services/xiyan_sql/tests/ -v
