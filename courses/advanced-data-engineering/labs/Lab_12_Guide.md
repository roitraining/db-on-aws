# Lab 12: Bundle the Pipeline and Deploy

**Course:** Databricks on AWS: Advanced Data Engineering
**Duration:** 60 minutes

**Course Repository:** https://github.com/roitraining/db-on-aws

---

## Overview

Everything you built over two days exists because you clicked it into being. This lab turns it into code: a `databricks.yml` manifest declaring your pipeline and Job, with dev and prod targets pointing at different catalogs, deployable from the GitLab repository you linked in Lab 7.

---

## Prerequisites

- [ ] Labs 7–11 completed — the pipeline and Job both exist
- [ ] Databricks CLI installed locally and authenticated to the workspace
- [ ] **CLI v1.0 or later** — check with `databricks --version` before you start
- [ ] The GitLab repository from Lab 7, cloned locally
- [ ] Two catalogs available for dev and prod targets

> **Common Pitfall:** Verify the CLI version first. Bundle commands on a CLI from the 0.x line shell out to Terraform, and those builds fail before doing any work with `error downloading Terraform: unable to verify checksums signature: openpgp: key expired`. It reads like a network or proxy fault and it is neither — it is an expired signing key baked into an old binary, and the only fix is upgrading the CLI. This was reproduced on v0.224.0 and resolved on v1.14.1.

---

## Objectives

- Initialise a bundle and explain each top-level mapping in `databricks.yml`
- Declare an existing pipeline and Job as bundle resources
- Define dev and prod targets with per-target catalog overrides
- Run `validate` and read what it checks
- Deploy to the dev target and verify in the workspace
- Capture an existing workspace resource as code with `bundle generate`
- Explain what `deployment bind` does and why it matters

---

## Part 1: The Manifest

### Task 1: Initialise

1. **Create a bundle in your repository**

    From the local clone of your Lab 7 repository:

    ```bash
    databricks bundle init
    ```
    <!-- source: facts_extracted.md §8 -->

2. **Open the generated `databricks.yml`**

    This is the manifest. Everything the bundle deploys is declared here or in files it references.
    <!-- source: facts_extracted.md §8 -->

3. **Read the minimal form**

    ```yaml
    bundle:
      name: my_bundle

    targets:
      dev:
        default: true
    ```
    <!-- source: facts_extracted.md §8 -->

4. **Identify the four top-level mappings**

    | Mapping | Purpose |
    |---|---|
    | `bundle` | Required. Bundle name and core settings including `databricks_cli_version` |
    | `targets` | Deployment environments with configuration overrides. **One must have `default: true`** |
    | `resources` | Jobs, clusters, dashboards, pipelines and other managed assets |
    | `variables` | Reusable values with optional descriptions, defaults and types |
    <!-- source: facts_extracted.md §8 -->

    > **Note:** The product is **Declarative Automation Bundles**, formerly Databricks Asset Bundles. The abbreviation DAB, the `databricks.yml` filename, and the `validate` and `deploy` verbs are all unchanged. You will see the old name in most existing material.
    <!-- source: facts_extracted.md §8 -->

---

## Part 2: Declare Your Resources

### Task 2: Targets

5. **Define dev and prod targets**

    ```yaml
    targets:
      dev:
        default: true
        variables:
          target_catalog: eng_<id>
      prod:
        workspace:
          host: https://<production-workspace-url>
        variables:
          target_catalog: prod_analytics
    ```
    <!-- source: facts_extracted.md §8 -->

6. **Declare the catalog as a variable**

    ```yaml
    variables:
      target_catalog:
        description: Catalog the pipeline writes into
        default: eng_<id>
    ```
    <!-- source: facts_extracted.md §8 -->

    > **Key Insight:** The environments differ by data, not by code. One manifest, one pipeline definition, and a variable that changes where it writes. The moment you maintain two copies of a pipeline for two environments, they begin to drift, and the drift is always discovered in production.

### Task 3: Resources

7. **Declare the Lab 10 pipeline**

    ```yaml
    resources:
      pipelines:
        medallion_pipeline:
          name: medallion_<id>
          catalog: ${var.target_catalog}
          libraries:
            - notebook:
                path: ./pipelines/medallion.py
    ```
    <!-- source: facts_extracted.md §8 -->

8. **Declare the Lab 11 Job**

    ```yaml
      jobs:
        medallion_job:
          name: medallion_job_<id>
          tasks:
            - task_key: run_pipeline
              pipeline_task:
                pipeline_id: ${resources.pipelines.medallion_pipeline.id}
    ```
    <!-- source: facts_extracted.md §8 -->

9. **Note the cross-reference**

    > **Key Insight:** The Job refers to the pipeline by bundle reference, not by a hard-coded ID. Deploy to dev and it binds to the dev pipeline; deploy to prod and it binds to the prod one. A hard-coded ID would silently point production at a development pipeline — and would deploy successfully while doing so.

---

## Part 3: Validate and Deploy

### Task 4: Validate

10. **Run validate**

    ```bash
    databricks bundle validate
    ```
    <!-- source: facts_extracted.md §8 -->

11. **Read what it reports**

    Validation checks the manifest's structure and resolves variables and references. It does not deploy.

12. **Break something deliberately**

    Change a resource reference to a name that does not exist and re-run `validate`.

13. **Fix it and re-validate**

    > **Key Insight:** Validate is what runs on a pull request. It is cheap, it touches nothing, and it catches the class of error that would otherwise fail halfway through a production deployment leaving resources half-created.

### Task 5: Deploy

14. **Deploy to dev**

    ```bash
    databricks bundle deploy -t dev
    ```
    <!-- source: facts_extracted.md §8 -->

15. **Verify in the workspace**

    Confirm the pipeline and Job appear, named per your bundle, writing to the dev catalog.

16. **Confirm the deployed Job is bundle-managed**

    Note the indication in the workspace that this resource is managed by a bundle rather than edited directly.

    > **Expected Result:** A pipeline and a Job in the workspace that you did not click into existence, pointing at the dev catalog because the target said so.

---

## Part 4: Bring Existing Resources Under Control

### Task 6: Generate and Bind

17. **Generate configuration from an existing workspace resource**

    ```bash
    databricks bundle generate job --existing-job-id <job-id>
    ```
    <!-- source: facts_extracted.md §8 -->

18. **Read the generated YAML**

    Compare it against the Job you hand-wrote in Task 3. Two things in it matter more than the rest.

    First, the command writes to `resources/<name>.job.yml` and then warns you that the file **is not part of your bundle**:

    ```
    Warning: Generated configuration is not included in the bundle
    ```
    <!-- source: facts_extracted.md §8 -->

    Generated configuration is inert until an `include` pattern picks it up. Add one:

    ```yaml
    include:
      - resources/*.yml
    ```
    <!-- source: facts_extracted.md §8 -->

    Second, look at how the generated Job refers to the pipeline:

    ```yaml
    pipeline_task:
      pipeline_id: 013ed010-e086-427b-acf0-c283466b334e
    ```
    <!-- source: facts_extracted.md §8 -->

    > **Key Insight:** That is a hard-coded ID — precisely the thing step 9 warned you never to write. `generate` reports what the workspace currently holds, and the workspace holds a resolved ID, so a faithful capture of reality is also an anti-pattern. Replace it with `${resources.pipelines.medallion_pipeline.id}` by hand before you commit. This is the single most important habit in the lab: `generate` gets you 90% of the way and hands you the last 10% as a trap that deploys perfectly to dev and quietly points prod at your development pipeline.

19. **Confirm the deployment metadata**

    The generated YAML also carries the fields that mark the resource as bundle-managed rather than hand-edited:

    ```yaml
    deployment:
      kind: BUNDLE
    edit_mode: UI_LOCKED
    ```
    <!-- source: facts_extracted.md §8 -->

    `UI_LOCKED` is why the workspace UI stops you editing a deployed Job directly. The manifest is the source of truth, and the UI enforces it.

20. **Understand `deployment bind`**

    Binding places an already-running workspace resource under bundle control without recreating it.
    <!-- source: facts_extracted.md §8 -->

    > **Key Insight:** This is the migration path, and it is the reason bundles are adoptable at all. You do not have to stop your running jobs, rebuild them as code, and cut over. You generate the configuration from what exists, bind it, and it is under version control from the next deploy onward — with no downtime and no re-creation.

21. **Sketch the GitLab CI flow**

    Write the three stages the approved outline describes: validate on pull request, deploy to dev on merge, promote to prod on release tag — with the service principal authenticating from GitLab CI variables.

    > **Key Insight:** The service principal from Lab 11 is the same identity that runs the deployment. Your personal credentials appear nowhere in CI, which is what makes the pipeline survive your holiday and satisfy an auditor.

22. **Commit the bundle**

    Commit `databricks.yml` and the pipeline source to your branch and push.

---

## Stretch Task

1. Add a second Job to the bundle that runs only in prod, using a target-specific resource override. What did that require, and what does it suggest about how far targets can diverge before you have two pipelines again?
2. Write the `.gitlab-ci.yml` implementing the three stages. Where does the service principal credential live, and what happens if the release tag is malformed?
3. Deploy to dev, change the pipeline in the workspace UI, then re-deploy. What happened to your manual change, and what does that tell you about who owns a bundle-managed resource?

---

## Checkpoint: Verify Your Progress

- [ ] I initialised a bundle in my repository
- [ ] I can name all four top-level mappings and what each does
- [ ] I know which target must carry `default: true`
- [ ] I defined dev and prod targets
- [ ] I declared the catalog as a variable overridden per target
- [ ] I declared the Lab 10 pipeline as a resource
- [ ] I declared the Lab 11 Job as a resource
- [ ] The Job references the pipeline by bundle reference, not a hard-coded ID
- [ ] I ran `validate` successfully
- [ ] I broke a reference, saw validate catch it, and fixed it
- [ ] I deployed to the dev target
- [ ] I verified the pipeline and Job in the workspace
- [ ] I generated configuration from an existing workspace resource
- [ ] I can explain what `deployment bind` does and why it matters
- [ ] I sketched the three-stage GitLab CI flow
- [ ] I committed the bundle to my branch

---

## Troubleshooting Reference

> **Key Insight:** Bundle errors are almost always in the manifest rather than the workspace. `validate` resolves variables and references without touching anything, so run it before every deploy and read what it says.
<!-- source: facts_extracted.md §8 -->

| Issue | Symptom | Solution |
|---|---|---|
| `validate` fails on a variable | Unresolved reference | The variable is not declared, or the target does not override it. |
| No default target | Error naming targets | Exactly one target must carry `default: true`. |
| Deploy targets the wrong workspace | Resources appear in the wrong place | The `-t` flag was omitted, so the default target was used. |
| Job cannot find the pipeline | Reference error at run time | A hard-coded pipeline ID rather than a bundle reference. |
| Deploy overwrites a manual change | UI edits disappear | Expected. A bundle-managed resource is owned by the manifest. |
| Authentication fails in CI | Deploy fails only in CI | The service principal credential is missing or unscoped in GitLab CI variables. |
| `bundle generate` output does not match | Generated YAML differs from yours | Expected — it reflects the resource's actual state, which may have drifted. |

---

## Cost Considerations

| Resource | Driver | Control |
|---|---|---|
| Deployed pipeline and Job | Cost when they run, not when declared | Deploy freely to dev; keep schedules paused until intended. |
| Duplicate dev and prod resources | Two environments, two sets | Targets exist so they are cheap to keep separate. Do not run both on a schedule during the course. |
| `validate` | Free — touches nothing | Run it constantly. |

**Cleanup:** Destroy the dev deployment at the end of the course, or pause all schedules. Keep the repository — it is the deliverable you take away.

---

## Knowledge Check

1. Name the four top-level mappings in `databricks.yml` and what each is for.
2. What is the constraint on targets, and what happens if it is violated?
3. Your dev and prod pipelines are identical except for the catalog. How is that expressed, and why is a second copy the wrong answer?
4. What does `validate` check, and why is it the right thing to run on a pull request?
5. The Job references the pipeline by bundle reference rather than ID. What failure does that prevent, and why would that failure be hard to spot?
6. You have thirty running jobs built by hand. What is the adoption path that does not require recreating them?
7. Someone edits a bundle-managed job in the UI and the next deploy reverts it. Is that a bug? Justify your answer.
8. Which identity authenticates the deploy in CI, and why not a person's?

Answers are held in the Knowledge Check Bank.

---

## Next Steps

This is the final lab. In the wrap-up, map the four days back onto your migration roadmap: what each team member owns, what the analysts from Days 1–2 can now do without you, and which open questions to take to IT.

---

## Resources

- Declarative Automation Bundles: https://docs.databricks.com/aws/en/dev-tools/bundles/
- Bundle settings: https://docs.databricks.com/aws/en/dev-tools/bundles/settings
- Databricks Git folders: https://docs.databricks.com/aws/en/repos/
- Databricks on AWS documentation: https://docs.databricks.com/aws/en/

---

*Lab 12 Complete*
