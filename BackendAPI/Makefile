.PHONY: ingest-excels ingest-excels-dry-run seed-demo-user

# Usage examples:
#   make ingest-excels ORG_ID=<ORG_UUID> CLOUD_ACCOUNT_ID=<ACCOUNT_UUID>
#   make ingest-excels-dry-run ORG_ID=<ORG_UUID> CLOUD_ACCOUNT_ID=<ACCOUNT_UUID>
#
# Optional:
#   INPUT_DIR=../../attachments
#   DATABASE_URL=postgresql://...

PYTHON ?= python
INPUT_DIR ?= ../../attachments

ingest-excels:
	$(PYTHON) scripts/ingest_datasets.py --org "$(ORG_ID)" --cloud-account-id "$(CLOUD_ACCOUNT_ID)" --input-dir "$(INPUT_DIR)"

ingest-excels-dry-run:
	$(PYTHON) scripts/ingest_datasets.py --org "$(ORG_ID)" --cloud-account-id "$(CLOUD_ACCOUNT_ID)" --input-dir "$(INPUT_DIR)" --dry-run

seed-demo-user:
	$(PYTHON) scripts/seed_demo_user.py
