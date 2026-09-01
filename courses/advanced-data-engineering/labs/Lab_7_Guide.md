# Lab 7: Governance Foundations

**Course:** Databricks on AWS: Advanced Data Engineering
**Duration:** 60 minutes

**Course Repository:** https://github.com/roitraining/db-on-aws

---

## Overview

You sat the Intro course as an analyst. Now you own the platform. This lab sets up the governed workspace you will build everything else in: your own catalog, a peer grant you verify from both sides, your GitLab repository linked in, and an external location over S3 so you can see exactly where a managed table differs from an external one.

---

## Prerequisites

- [ ] Intro Labs 1–6 completed
- [ ] `CREATE CATALOG` on the metastore, or an instructor who has it
- [ ] S3 bucket name and IAM role ARN supplied by your instructor
- [ ] GitLab repository URL and a personal access token
- [ ] A peer in the room to verify your grants

---

## Objectives

- Create a catalog and schema you own
- Grant a peer the three privileges required to read, and verify from their side
- Link a GitLab repository as a Databricks Git folder and commit from the UI
- Create a storage credential and an external location over S3
- Create an external table and state precisely how it differs from a managed table

---

## Part 1: Your Own Catalog

### Task 1: Create and Populate

1. **Create your catalog and schema**

    Substitute your engineer ID throughout.

    ```sql
    CREATE CATALOG IF NOT EXISTS eng_<id>;
    CREATE SCHEMA  IF NOT EXISTS eng_<id>.work;
    ```
    <!-- source: facts_extracted.md §1 -->

    > **Common Pitfall:** On some accounts the first statement fails with
    > **`Metastore storage root URL does not exist`**, often mentioning that Default Storage is
    > enabled. The metastore has no default managed location, so Unity Catalog does not know where
    > to put the catalog's data. Your syntax and your permissions are both fine. Name the location
    > explicitly instead:
    >
    > ```sql
    > CREATE CATALOG IF NOT EXISTS eng_<id>
    > MANAGED LOCATION 's3://<bucket>/<prefix>/eng_<id>';
    > ```
    >
    > The path must sit inside an external location you are allowed to use. Your instructor will
    > tell you which form this workspace needs — worth settling before the whole room hits it at once.
    <!-- source: facts_extracted.md §1 -->

2. **Create a managed table from the training data**

    ```sql
    CREATE OR REPLACE TABLE eng_<id>.work.institutions_managed AS
    SELECT * FROM training_nic.migrated.institutions;
    ```
    <!-- source: facts_extracted.md §9 -->

3. **Inspect where it physically lives**

    ```sql
    DESCRIBE EXTENDED eng_<id>.work.institutions_managed;
    ```
    <!-- source: facts_extracted.md §1 -->

    > **Note:** Record the `Location` value. A managed table lives in Unity Catalog's managed storage and Unity Catalog owns its lifecycle — drop the table and the data goes with it.

---

## Part 2: Grant and Verify

### Task 2: The Three Privileges

4. **Grant SELECT alone**

    ```sql
    GRANT SELECT ON TABLE eng_<id>.work.institutions_managed TO `<peer>`;
    ```
    <!-- source: facts_extracted.md §1 -->

5. **Have your peer attempt to read it**

    It will fail. `USE CATALOG` is a traversal privilege that grants access to nothing by itself.
    <!-- source: facts_extracted.md §1 -->

6. **Grant the traversal privileges**

    ```sql
    GRANT USE CATALOG ON CATALOG eng_<id> TO `<peer>`;
    GRANT USE SCHEMA  ON SCHEMA  eng_<id>.work TO `<peer>`;
    ```
    <!-- source: facts_extracted.md §1 -->

7. **Have your peer retry, then audit**

    ```sql
    SHOW GRANTS ON TABLE eng_<id>.work.institutions_managed;
    ```
    <!-- source: facts_extracted.md §1 -->

    > **Key Insight:** Your analysts hit this in Intro Lab 5 and it looked like a bug. As the platform owner you are the person they will ask. The answer is always the same: check `USE SCHEMA` first.

---

## Part 3: Git Integration

### Task 3: Link GitLab

8. **Create a Git folder**

    In **Workspace**, create a Git folder pointing at your GitLab repository URL, authenticating with your personal access token.

9. **Create a working branch**

    Use a branch named for you rather than committing to the default branch. Pipeline development happens on branches; `main` is what gets deployed.

10. **Add a file and commit**

    Create `pipelines/README.md` describing what this repository will hold, then **Commit and Push**.

    > **Note:** In Lab 12 this same repository becomes the source of a Declarative Automation Bundle. What you commit here is the beginning of that.

---

## Part 4: External Storage

### Task 4: Storage Credential and External Location

11. **Create a storage credential in Catalog Explorer**

    A storage credential wraps the IAM role Databricks assumes to reach your bucket. **There is no SQL statement for this** — it is created in the UI or through the API.

    Go to **Catalog → Connect → Credentials → Create credential**, choose credential type **AWS IAM Role**, and enter a name of `cred_<id>` plus the IAM Role ARN your instructor supplied.
    <!-- source: facts_extracted.md §1 -->

    > **Note:** Copy the **External ID** shown after creation. It completes the trust relationship on the AWS side. Creating the credential requires `CREATE STORAGE CREDENTIAL` on the metastore.

    > **Key Insight:** Notice you were sent to the UI. Almost everything else in Unity Catalog is SQL, and it is worth asking why this is not. A credential is a secret-bearing object with an AWS-side handshake, so it deliberately does not live in a statement you might paste into a shared notebook.

12. **Create an external location over the bucket path — this part is SQL**

    ```sql
    CREATE EXTERNAL LOCATION IF NOT EXISTS `loc_<id>`
    URL 's3://<bucket>/<prefix>/<id>'
    WITH (STORAGE CREDENTIAL `cred_<id>`)
    COMMENT 'Lab 7 external location';
    ```
    <!-- source: facts_extracted.md §1 -->

    > **Note:** This needs `CREATE EXTERNAL LOCATION` on **both** the metastore and the storage credential. Two objects, two privileges — the credential says *how* to authenticate, the location says *what path* that credential is allowed to cover.

13. **Verify the location is reachable**

    ```sql
    LIST 's3://<bucket>/<prefix>/<id>';
    ```
    <!-- source: facts_extracted.md §1 -->

    > **Common Pitfall:** Read the error text before you react. Your prefix is empty at this point, so `LIST` returns **`No such file or directory`** — that is the expected result here and it is *not* a permissions failure. It proves the credential worked: Databricks reached S3 and found nothing there. You will see files appear at this same path in Task 5.

    > **Troubleshooting:** A genuine failure reads as an *access* or *access denied* error rather than a missing path, and it is an IAM trust or bucket policy problem, not a Unity Catalog problem. The role must trust Databricks and permit the bucket path.

### Task 5: External vs Managed

14. **Create an external table at that path**

    ```sql
    CREATE OR REPLACE TABLE eng_<id>.work.institutions_external
    LOCATION 's3://<bucket>/<prefix>/<id>/institutions'
    AS SELECT * FROM training_nic.migrated.institutions;
    ```
    <!-- source: facts_extracted.md §1 -->

15. **Compare the two tables**

    ```sql
    DESCRIBE EXTENDED eng_<id>.work.institutions_external;
    ```
    <!-- source: facts_extracted.md §1 -->

16. **Record the difference**

    Write down the `Location` and `Type` for both tables.

    > **Key Insight:** The difference that matters is lifecycle, not location. Dropping a managed table removes the data. Dropping an external table removes the metadata and leaves the files. For a migration where another system still reads those files, that distinction decides which kind you create.

    > **Expected Result:** Two tables with identical contents, different `Type` values, and different `Location` values.

---

## Stretch Task

1. Drop the external table, confirm the S3 files survive, then recreate the table over the same path without re-reading the source.
2. Grant your peer `READ FILES` on the external location but not `SELECT` on the table. What can they do, and what does that tell you about the two permission systems?
3. Write the `SHOW GRANTS` statements needed to audit catalog, schema, and table in one pass, and describe how you would spot an over-permissioned principal.

---

## Checkpoint: Verify Your Progress

- [ ] I created my own catalog and schema
- [ ] I created a managed table and recorded its location
- [ ] I granted `SELECT` alone and confirmed my peer could not read
- [ ] I added `USE CATALOG` and `USE SCHEMA` and my peer succeeded
- [ ] I verified a peer's table from my own account
- [ ] I ran `SHOW GRANTS` and read the output
- [ ] I linked a GitLab repository as a Git folder
- [ ] I worked on a branch rather than the default
- [ ] I committed and pushed a file from the Databricks UI
- [ ] I created a storage credential
- [ ] I created an external location and listed its contents
- [ ] I created an external table at that path
- [ ] I recorded `Type` and `Location` for both tables
- [ ] I can state what happens to the data when each table is dropped

---

## Troubleshooting Reference

> **Key Insight:** Storage failures and catalog failures look alike from the error message but have nothing to do with each other. If `LIST` on the path fails, the problem is IAM. If `LIST` works and `SELECT` fails, the problem is a grant.
<!-- source: facts_extracted.md §1 -->

| Issue | Symptom | Solution |
|---|---|---|
| Cannot create a catalog | Permission denied | You lack `CREATE CATALOG` on the metastore. Ask your instructor. |
| Peer cannot read the table | Access denied with `SELECT` granted | Missing `USE CATALOG` or `USE SCHEMA`. |
| Storage credential creation fails | Error on the IAM role | The role ARN is wrong or does not trust Databricks. |
| `LIST` on the S3 path fails | Access denied error | IAM trust or bucket policy, not Unity Catalog. |
| `LIST` says no such file or directory | Missing path, not denied | Expected on an empty prefix. The credential worked; there is simply nothing there yet. |
| External location overlaps another | `INVALID_PARAMETER_VALUE.LOCATION_OVERLAP` | External locations cannot overlap, and that includes nesting inside an existing one. Use a prefix unique to you, outside any existing location. |
| Git push rejected | Authentication failure | The PAT is expired or lacks write scope on the repository. |
| External table creation fails | Path error | The path must sit inside an external location you have permission on. |

---

## Cost Considerations

| Resource | Driver | Control |
|---|---|---|
| Cluster or warehouse | Billed while running | Let it auto-terminate. |
| Table copies | Two full copies of the source table | Small at training scale; drop both at course end. |
| S3 storage | External table files persist after the table is dropped | Delete the prefix explicitly during cleanup. |

**Cleanup:** Keep the catalog, schema and Git folder — Labs 8–12 all build on them. Drop the two `institutions_*` tables at the end of the course and remove the S3 prefix.

---

## Knowledge Check

1. Name the three privileges required to read a table and state which is most often missing.
2. What does a storage credential wrap, and what does an external location add on top of it?
3. You drop a managed table and an external table. What happens to the data in each case?
4. Your peer can `LIST` the S3 path but cannot `SELECT` from the external table over it. Which system is refusing, and why are they separate?
5. Why commit pipeline work on a branch rather than the default branch?
6. During a migration, when would you deliberately choose an external table over a managed one?

Answers are held in the Knowledge Check Bank.

---

## Next Steps

Lab 8 moves from platform setup to code. You will convert a multi-join T-SQL stored procedure into PySpark and use the Spark UI to work out why the first version is slow.

---

## Resources

- Unity Catalog privileges: https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges
- Catalog Explorer: https://docs.databricks.com/aws/en/catalog-explorer/
- Databricks Git folders: https://docs.databricks.com/aws/en/repos/

---

*Lab 7 Complete*
