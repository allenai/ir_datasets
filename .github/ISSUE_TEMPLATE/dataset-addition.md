---
name: Dataset Addition
about: Propose adding a new dataset, collection of related datasets, or feature to
  existing dataset
title: ''
labels: add-dataset
assignees: ''

---

**Dataset Information:**

<brief description>

**Links to Resources:**

<links including data websites, repositories, papers, etc. that would help in adding the dataset.>

**Dataset ID(s) & supported entities:**

 - <propose dataset ID(s), and where they fit in the hierarchy, and specify which entity types each will provide (docs, queries, qrels, scoreddocs, docpairs, qlogs)>

**Checklist**

Mark each task once completed. All should be checked prior to merging a new dataset.
  
 - [ ] Dataset definition (in `ir_datasets/datasets/[topid].py`)
 - [ ] Tests (in `tests/integration/[topid].py`)
 - [ ] Metadata generated (using `ir_datasets generate_metadata` command, should appear in `ir_datasets/etc/metadata.json`)
 - [ ] Documentation (in `ir_datasets/etc/[topid].yaml`)
   - [ ] Documentation generated in https://github.com/seanmacavaney/ir-datasets.com/
 - [ ] Downloadable content (in `ir_datasets/etc/downloads.json`)
   - [ ] Download verification action (in `.github/workflows/verify_downloads.yml`). Only one needed per `topid`.
   - [ ] Any small public files from NIST (or other potentially troublesome files) mirrored in https://github.com/seanmacavaney/irds-mirror/. Mirrored status properly reflected in `downloads.json`.
  
**Additional comments/concerns/ideas/etc.**
