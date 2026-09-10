# Lab 0: Set Up Your Workspace

**Course:** Databricks on AWS: Cloud Analytics for Business Users
**Duration:** 15 minutes

**Course Repository:** https://github.com/roitraining/db-on-aws

---

## Overview

Before Lab 1, you will set up your own workspace: add the course repository so every lab guide is
inside Databricks, and build the `training_nic` catalog you will query throughout the course. You
do every step yourself, in your own account.

---

## Prerequisites

- [ ] Your workspace URL, username, and password, provided by your instructor
- [ ] A browser signed out of any other Databricks workspace

---

## Objectives

- Sign in to your Databricks workspace
- Add the course repository as a Git folder
- Run the environment setup notebook to build the `training_nic` catalog
- Confirm the migrated data is present

---

## Part 1: Add the Course Repository

### Task 1: Sign In

1. Open the **workspace URL** your instructor gave you in a browser.

2. Sign in with your **username and password**.

### Task 2: Add the Repository as a Git Folder

3. In the left sidebar, click **Workspace**.

4. Navigate to **Workspace → Users → your.email@company.com** (your own user folder).

5. Click **Create** (top right) and choose **Git folder**.

6. Fill in the dialog exactly:

    | Field | Value |
    |---|---|
    | Git repository URL | `https://github.com/roitraining/db-on-aws` |
    | Git provider | **GitHub** |
    | Git folder name | `db-on-aws` (fills in automatically) |

7. Click **Create Git folder.**

    > The repository is **public**, so you will **not** be asked for a token or password. If you
    > are, stop and tell your instructor — do not create a token.

8. Confirm the clone worked. Under your user folder you should now see:

    ```
    db-on-aws/
      courses/
      bundles/
      reference/
    ```

---

## Part 2: Build Your Environment

### Task 3: Open the Setup Notebook

9. In your `db-on-aws` Git folder, open
   **`courses/cloud-analytics-business-users/setup/00_environment_setup.py`**.

    Click the file to open it as a notebook.

### Task 4: Attach Compute and Run

10. At the **top right** of the notebook, click the compute selector and attach a **serverless**
    notebook cluster.

11. At the top of the notebook, click **Run All.**

12. Wait for it to finish (about 2–3 minutes). The last cell prints:

    ```
    All setup checks passed. Environment is ready for Labs 1-6.
    ```

    > **Expected Result:** the "All setup checks passed" message. The notebook stops with an error
    > if anything is wrong, so a clean finish is a real signal that your environment is ready.

---

## Part 3: Confirm

### Task 5: Verify the Catalog

13. In the left sidebar, click **SQL Editor**.

14. In the warehouse selector (top right), choose your **serverless SQL warehouse**.

15. Run:

    ```sql
    SELECT COUNT(*) FROM training_nic.migrated.institutions;
    ```

16. Confirm the result is **4900**.

---

## Checkpoint: Verify Your Progress

- [ ] The `db-on-aws` Git folder is in your workspace
- [ ] The setup notebook finished with "All setup checks passed"
- [ ] `training_nic` appears in Catalog Explorer (**Catalog** in the sidebar)
- [ ] `SELECT COUNT(*) FROM training_nic.migrated.institutions` returns 4900

If all four are ticked, you are ready for **Lab 1**.

---

## Troubleshooting Reference

| Symptom | Cause | Fix |
|---|---|---|
| No serverless compute in the dropdown | Not enabled for your account | Ask your instructor |
| `Metastore storage root URL does not exist … Default Storage` | The metastore has no default managed location | Ask your instructor for an S3 path, run `CREATE CATALOG training_nic MANAGED LOCATION 's3://<bucket>/<prefix>/training_nic';`, then **Run All** again |
| Prompted for a GitHub token | Repo treated as private | The repo is public — tell your instructor rather than creating a token |
| `training_nic` still missing after a run | The notebook did not finish | Re-open it, confirm it ends with "All setup checks passed" |

---

## Next Steps

Your environment is built. Continue to **Lab 1: Orientation and Your First Query**.
