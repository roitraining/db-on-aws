# Lab 7: Governance Foundations

**Course:** Databricks on AWS: Advanced Data Engineering
**Duration:** 60 minutes

**Course Repository:** https://github.com/roitraining/db-on-aws

---

## Overview

You sat the Intro course as an analyst. Now you own the platform. This lab sets up the governed workspace you will build everything else in: your own catalog, grants shaped the way an owner actually maintains them, row filters and column masks that change what a query returns without changing the query, your GitLab repository linked in, and an external location over S3 so you can see exactly where a managed table differs from an external one.

---

## Prerequisites

- [ ] **[`SETUP.md`](../../../SETUP.md) Parts 1 and 2 completed**—course material added as a Git folder, compute selected
- [ ] **The `training_nic` environment is built**—an instructor runs the notebook in [`../setup/`](../setup/) once before class (see its [`README`](../setup/README.md)). Quick check: `SELECT COUNT(*) FROM training_nic.perf.institutions_large` returns 2,000,000
- [ ] Intro Labs 1–6 completed, **or** `SETUP.md` Part 4 worked through—its first two steps are Parts 1 and 2 (Git folder, compute); see below
- [ ] `CREATE CATALOG` on the metastore, or an instructor who has it
- [ ] S3 bucket name and IAM role ARN supplied by your instructor — in the ROI class workspace: bucket `roi-databricks-demo-data`, role `arn:aws:iam::029331796573:role/roi-databricks-uc-demo`
- [ ] GitLab repository URL and a personal access token

> **Did not take the Intro course?** You are not stuck, but do not skip this. Labs 7–12 assume
> Labs 1–6 and deliberately do not reteach them. Work through **[`SETUP.md`](../../../SETUP.md)
> Part 4—Advanced course catch-up** before starting. Its first two steps send you through Parts
> 1 and 2—adding the course repo as a Git folder, and picking compute—and the rest is a
> ten-minute read covering the four things this
> lab assumes you already know: the three-level namespace, the leading `#` on the NIC key and why
> it needs backticks, the three privileges required to read a table, and what the `legacy_onprem`
> and `migrated` schemas represent. Task 2 of this lab in particular will look like a bug if you
> have not met the third of those.
>
> One further heads-up: **Lab 8 builds directly on Intro Lab 4.** If you are not fluent in the
> DataFrame API, skim `courses/cloud-analytics-business-users/labs/Lab_4_Guide.md` tonight—about twenty minutes. Everything else in Labs 7–12 is self-contained.

> **Note on the two repositories in this course.** The **course repository** is the public GitHub
> repo you added in `SETUP.md` Part 1, holding these guides; it needs no credential. The **GitLab
> repository** in Task 3 below is a *separate*, private repo supplied by your instructor, which you
> link with a personal access token and commit to. Task 3 is about the credentialed workflow—do
> not confuse it with the one you already have.

---

## Objectives

- Create a catalog and schema you own
- Grant the three privileges required to read, and audit the complete chain at every level
- Grant at the schema level and prove new tables inherit it
- Read an object's owner and transfer ownership
- Attach a row filter and a column mask to a table, verify both, and remove them
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
    > MANAGED LOCATION 's3://roi-databricks-demo-data/eng-catalogs/eng_<id>';
    > ```
    >
    > The path must sit inside an external location you are allowed to use — in the ROI class
    > workspace that is `eng_catalogs`, over `s3://roi-databricks-demo-data/eng-catalogs/`,
    > pre-created by the instructor. **The ROI class workspace requires this form** — the plain
    > `CREATE CATALOG` fails there. In a different workspace, your instructor supplies the path.
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

    > **Note:** Note the `Location` value. A managed table lives in Unity Catalog's managed storage and Unity Catalog owns its lifecycle—drop the table and the data goes with it.

---

## Part 2: Grant and Verify

### Task 2: The Three Privileges

4. **Grant SELECT alone**

    Everyone here runs an isolated account, so the stand-in for a colleague is the built-in **`account users`** group—the mechanics are identical to granting one person.

    ```sql
    GRANT SELECT ON TABLE eng_<id>.work.institutions_managed TO `account users`;
    ```
    <!-- source: facts_extracted.md §1 -->

5. **Audit why that is not enough**

    A reader holding only that grant still fails—`USE CATALOG` and `USE SCHEMA` are traversal privileges, and neither has been granted. Prove the gap by auditing all three levels:

    ```sql
    SHOW GRANTS ON TABLE   eng_<id>.work.institutions_managed;
    SHOW GRANTS ON SCHEMA  eng_<id>.work;
    SHOW GRANTS ON CATALOG eng_<id>;
    ```
    <!-- source: facts_extracted.md §1 -->

    > **Expected Result:** `account users` holds `SELECT` at the table and appears nowhere above it.

6. **Grant the traversal privileges**

    ```sql
    GRANT USE CATALOG ON CATALOG eng_<id> TO `account users`;
    GRANT USE SCHEMA  ON SCHEMA  eng_<id>.work TO `account users`;
    ```
    <!-- source: facts_extracted.md §1 -->

7. **Re-audit the complete chain**

    Re-run the three `SHOW GRANTS` statements—`account users` now appears at every level.

    > **Note:** In a shared class workspace you would verify from a real peer's session—grant, watch them fail, grant traversal, watch them succeed. That cross-check needs two people in one workspace; your instructor may demonstrate it there.
    <!-- source: facts_extracted.md §1 -->

    > **Key Insight:** Your analysts hit this in Intro Lab 5 and it looked like a bug. As the platform owner you are the person they will ask. The answer is always the same: check `USE SCHEMA` first.

8. **Grant at the schema, not the table**

    Per-table, per-person grants are how an audit dies. The owner's pattern is one grant at the
    schema, which covers every table in it — including tables that do not exist yet. Still in
    the SQL editor, grant read on the whole schema, then create a table that did not exist when
    you granted:

    ```sql
    GRANT SELECT ON SCHEMA eng_<id>.work TO `account users`;

    CREATE OR REPLACE TABLE eng_<id>.work.branch_counts AS
    SELECT STATE_ABBR_NM, COUNT(*) AS institutions
    FROM training_nic.migrated.institutions
    GROUP BY STATE_ABBR_NM;

    SHOW GRANTS ON TABLE eng_<id>.work.branch_counts;
    ```
    <!-- source: facts_extracted.md §1 -->

    > **Expected Result:** The brand-new table's grant list already shows
    > `account users | SELECT | SCHEMA | eng_<id>.work` — a privilege it inherited. You granted
    > nothing on the table itself, and the audit output says so: the securable column reads
    > `SCHEMA`, not `TABLE`.

    > **Key Insight:** Inheritance is why the three-privilege chain above is not the pattern
    > you use day to day. Grant `USE CATALOG` once, `SELECT` at the schema, and every future
    > table is covered — one line in the audit instead of hundreds.

    Now take the schema grant back — inheritance cuts both ways, and Lab 10 depends on it
    being gone:

    ```sql
    REVOKE SELECT ON SCHEMA eng_<id>.work FROM `account users`;
    ```

    > **Note:** Leave this grant in place and every table your Lab 10 pipeline creates —
    > including raw Bronze — is readable by everyone, which defeats that lab's publish-Gold-only
    > exercise. A schema grant covering tables that do not exist yet is exactly as powerful as
    > it is dangerous.

9. **Read ownership, then transfer it**

    Every Unity Catalog object has exactly one owner, and some operations are owner-only —
    Lab 11 meets one (`event_log()`) the hard way. Check who owns your table, hand it to a
    group, and take it back (substitute your login email in the last statement):

    ```sql
    SELECT table_owner
    FROM eng_<id>.information_schema.tables
    WHERE table_schema = 'work' AND table_name = 'institutions_managed';

    ALTER TABLE eng_<id>.work.institutions_managed SET OWNER TO `account users`;

    -- re-run the owner query above, then restore yourself:
    ALTER TABLE eng_<id>.work.institutions_managed SET OWNER TO `<your-email>`;
    ```
    <!-- source: facts_extracted.md §1 -->

    > **Expected Result:** `table_owner` reads your email, then `account users` after the
    > transfer, then your email again.

    > **Key Insight:** Ownership is transferable governance, and groups can own. Production
    > sets owners to a group or service principal so nothing is orphaned when a person leaves —
    > and so owner-only calls keep working. Remember this table exists when Lab 11's event-log
    > task fails under Run As.

---

## Part 3: Row Filters and Column Masks

The grants so far are all-or-nothing: a principal reads the table or does not. A migration
brings finer requirements — analysts see only their region, key columns are redacted outside
the owning team. In Unity Catalog those rules are **functions attached to the table**, so they
follow the data into every query, dashboard, and notebook — nobody has to remember a WHERE
clause. Everything in this part runs in the SQL editor on serverless, on any edition
(verified on Free Edition and the class workspace, 2026-09-30).

### Task 3: Policy on the Table, Not in the Query

10. **Attach a row filter**

    A row filter is a function returning a boolean; Unity Catalog evaluates it per row against
    the column you bind it to. Create one that admits only California, attach it, and run the
    same count before and after:

    ```sql
    SELECT COUNT(*) FROM eng_<id>.work.institutions_managed;   -- baseline: 61,699

    CREATE OR REPLACE FUNCTION eng_<id>.work.ca_only(state STRING)
    RETURN state = 'CA';

    ALTER TABLE eng_<id>.work.institutions_managed
      SET ROW FILTER eng_<id>.work.ca_only ON (STATE_ABBR_NM);

    SELECT COUNT(*) FROM eng_<id>.work.institutions_managed;   -- the SAME query again
    ```
    <!-- source: facts_extracted.md §1 -->

    > **Expected Result:** **61,699** before, **3,856** after — only California rows survive.
    > Confirm with `SELECT STATE_ABBR_NM, COUNT(*) FROM eng_<id>.work.institutions_managed
    > GROUP BY STATE_ABBR_NM;` — one row: `CA`.

    > **Note:** The filter applies to **you too**, the owner. That is the point — it is a
    > property of the table, not a courtesy the query extends.

11. **Mask the key column**

    A column mask is the same idea pointed at one column: a function that receives the
    column's value and returns what the reader is allowed to see. Redact the RSSD key:

    ```sql
    CREATE OR REPLACE FUNCTION eng_<id>.work.mask_key(k BIGINT)
    RETURN CAST(NULL AS BIGINT);

    ALTER TABLE eng_<id>.work.institutions_managed
      ALTER COLUMN `#ID_RSSD` SET MASK eng_<id>.work.mask_key;

    SELECT `#ID_RSSD`, NM_LGL, STATE_ABBR_NM
    FROM eng_<id>.work.institutions_managed
    LIMIT 5;
    ```
    <!-- source: facts_extracted.md §1 -->

    > **Expected Result:** Five rows, names and states intact, the `#ID_RSSD` column entirely
    > NULL. Note the backticks on `#ID_RSSD` in `ALTER COLUMN` — the native NIC name needs
    > them here exactly as it does in a SELECT.

    > **Common Pitfall:** The mask function's parameter type must match the column's type —
    > `#ID_RSSD` is `BIGINT`, so a mask declared over `STRING` refuses to attach. Check with
    > `DESCRIBE eng_<id>.work.institutions_managed` if in doubt.

12. **Update the policy without touching the table**

    An unconditional mask blinds everyone, including the pipeline that needs the key. Real
    policies branch on group membership inside the function — and because the mask is a
    function, you change the policy by replacing the function, with the table untouched:

    ```sql
    CREATE OR REPLACE FUNCTION eng_<id>.work.mask_key(k BIGINT)
    RETURN CASE WHEN is_account_group_member('admins') THEN k
                ELSE CAST(-1 AS BIGINT) END;

    SELECT `#ID_RSSD`, NM_LGL FROM eng_<id>.work.institutions_managed LIMIT 3;
    ```
    <!-- source: facts_extracted.md §1 -->

    > **Expected Result:** The key column now reads **-1** instead of NULL — the replaced
    > function took effect immediately, with no `ALTER TABLE`. (You still see the masked
    > value: the exemption checks an **account-level** group named `admins`, and being a
    > workspace admin does not put you in it.)

    > **Key Insight:** The policy moved into an object you can version, review, and audit.
    > `eng_<id>.information_schema.column_masks` and `...row_filters` list every attachment in
    > the catalog — that is the auditor's query.

13. **Remove both policies**

    Leave the table clean for the external-storage comparison in Part 5:

    ```sql
    ALTER TABLE eng_<id>.work.institutions_managed DROP ROW FILTER;
    ALTER TABLE eng_<id>.work.institutions_managed ALTER COLUMN `#ID_RSSD` DROP MASK;

    SELECT COUNT(*) FROM eng_<id>.work.institutions_managed;
    ```
    <!-- source: facts_extracted.md §1 -->

    > **Expected Result:** **61,699** again, with the key column populated. Nothing was ever
    > deleted — the filter removed *visibility*, not rows.

---

## Part 4: Git Integration

### Task 4: Link GitLab

14. **Create a Git folder**

    You did this once in `SETUP.md` Part 1 for the public course repo. This one differs in
    exactly one way — it is private, so it authenticates with your personal access token:

    1. In the left sidebar, click **Workspace**, and navigate to **Users → your.email**.
    2. Click **Create** (top right) and choose **Git folder**.
    3. **Git repository URL:** the GitLab URL your instructor supplied. **Git provider:**
       **GitLab**. Leave the folder name as it fills in.
    4. Click **Create Git folder**. When prompted for credentials, choose **GitLab**, enter
       your GitLab username, and paste the **personal access token** — not your GitLab
       password.

    > **Expected Result:** The folder appears with the repository's contents and a branch
    > button showing `main`.

15. **Create a working branch**

    Pipeline development happens on branches; `main` is what gets deployed.

    1. In the Workspace list view, hover over the Git folder's row and click the **Git**
       button that appears (it also lives behind the folder's **⋮ / kebab** menu → **Git…**).
    2. In the Git dialog, open the branch dropdown (showing `main`) and click
       **Create new branch**.
    3. Name it `dev-<id>` and create it. The dialog now shows `dev-<id>` as current.

16. **Add a file and commit**

    1. Inside the Git folder, click **Create → File**, name it `pipelines/README.md`, and
       write one line describing what this repository will hold.
    2. Open the **Git** dialog again — the new file is listed under changes.
    3. Enter a commit message and click **Commit & Push**.

    > **Expected Result:** The push succeeds, and the file is visible in GitLab's web UI on
    > the `dev-<id>` branch.

    > **Note:** In Lab 12 this same repository becomes the source of a Declarative Automation Bundle. What you commit here is the beginning of that.

---

## Part 5: External Storage

> **Free Edition cannot run Part 5.** Storage credentials and external locations need a
> customer-managed S3 bucket and IAM role, and Free Edition's storage is platform-managed—
> there is nothing to point a credential at and no permission to create one. This part runs
> in the shared class workspace, or as an instructor demo. Read it either way: the
> managed-versus-external distinction decides real migration behavior, and the knowledge
> check asks about it.

### Task 5: Storage Credential and External Location

17. **Create a storage credential in Catalog Explorer**

    A storage credential wraps the IAM role Databricks assumes to reach your bucket. **There is no SQL statement for this**—it is created in the UI or through the API.

    Go to **Catalog → Connect → Credentials → Create credential**, choose credential type **AWS IAM Role**, and enter a name of `cred_<id>` plus the IAM Role ARN your instructor supplied.
    <!-- source: facts_extracted.md §1 -->

    In the ROI class workspace the ARN is `arn:aws:iam::029331796573:role/roi-databricks-uc-demo`. Every attendee's credential wraps this same role — your isolation comes from the external location *path* you create next, not from the role.

    > **Note:** Copy the **External ID** shown after creation. It completes the trust relationship on the AWS side. Creating the credential requires `CREATE STORAGE CREDENTIAL` on the metastore.

    > **Key Insight:** Notice you were sent to the UI. Almost everything else in Unity Catalog is SQL, and it is worth asking why this is not. A credential is a secret-bearing object with an AWS-side handshake, so it deliberately does not live in a statement you might paste into a shared notebook.

18. **Create an external location over the bucket path—this part is SQL**

    ```sql
    CREATE EXTERNAL LOCATION IF NOT EXISTS `loc_<id>`
    URL 's3://roi-databricks-demo-data/lab7/<id>'
    WITH (STORAGE CREDENTIAL `cred_<id>`)
    COMMENT 'Lab 7 external location';
    ```
    <!-- source: facts_extracted.md §1 -->

    > **Note:** This needs `CREATE EXTERNAL LOCATION` on **both** the metastore and the storage credential. Two objects, two privileges—the credential says *how* to authenticate, the location says *what path* that credential is allowed to cover.

19. **Verify the location is reachable**

    ```sql
    LIST 's3://roi-databricks-demo-data/lab7/<id>';
    ```
    <!-- source: facts_extracted.md §1 -->

    > **Common Pitfall:** Read the error text before you react. Your prefix is empty at this point, so `LIST` returns **`No such file or directory`**—that is the expected result here and it is *not* a permissions failure. It proves the credential worked: Databricks reached S3 and found nothing there. You will see files appear at this same path in Task 6.

    > **Troubleshooting:** A genuine failure reads as an *access* or *access denied* error rather than a missing path. One shape to know: `UNAUTHORIZED_ACCESS ... statusCode: 403` on `LIST` run *before* the external location exists is **Unity Catalog** refusing — no external location covers that path yet — so create the location first. A 403 that persists *after* the location exists is an IAM trust or bucket policy problem: the role must trust Databricks and permit the bucket path.

### Task 6: External vs Managed

20. **Create an external table at that path**

    ```sql
    CREATE OR REPLACE TABLE eng_<id>.work.institutions_external
    LOCATION 's3://roi-databricks-demo-data/lab7/<id>/institutions'
    AS SELECT * FROM training_nic.migrated.institutions;
    ```
    <!-- source: facts_extracted.md §1 -->

21. **Compare the two tables**

    ```sql
    DESCRIBE EXTENDED eng_<id>.work.institutions_external;
    ```
    <!-- source: facts_extracted.md §1 -->

22. **Compare the two**

    Put the two `DESCRIBE EXTENDED` outputs side by side: `Type` is `MANAGED` vs `EXTERNAL`, and `Location` is Unity Catalog's managed storage vs your S3 path.

    > **Key Insight:** The difference that matters is lifecycle, not location. Dropping a managed table removes the data. Dropping an external table removes the metadata and leaves the files. For a migration where another system still reads those files, that distinction decides which kind you create.

    > **Expected Result:** Two tables with identical contents, different `Type` values, and different `Location` values.

23. **Grant the file layer separately from the table layer**

    The external location is its own securable with its own privileges. Grant read on the
    *files* and audit it:

    ```sql
    GRANT READ FILES ON EXTERNAL LOCATION `loc_<id>` TO `account users`;
    SHOW GRANTS ON EXTERNAL LOCATION `loc_<id>`;
    ```
    <!-- source: facts_extracted.md §1 -->

    > **Expected Result:** `account users | READ FILES | EXTERNAL LOCATION | loc_<id>`.

    > **Key Insight:** A holder of `READ FILES` can `LIST` and read the S3 path with no grant
    > on the table above it — and a holder of `SELECT` on the table needs no file privileges
    > at all. Two permission systems, deliberately separate. The troubleshooting rule from
    > this lab — *if `LIST` fails it is IAM, if `SELECT` fails it is a grant* — is the
    > operational face of that separation.

---

## Stretch Task

1. Drop the external table, confirm the S3 files survive, then recreate the table over the same path without rereading the source.
2. Rewrite `mask_key` so it redacts all but the last two digits of the key instead of hiding it entirely (keep the `BIGINT` return type). What does any mask do to a join on the masked column?
3. Write the `SHOW GRANTS` statements needed to audit catalog, schema, and table in one pass, and describe how you would spot an over-permissioned principal.

---

## Checkpoint: Verify Your Progress

- [ ] I created my own catalog and schema
- [ ] I created a managed table and recorded its location
- [ ] I granted `SELECT` alone and audited why it is not sufficient
- [ ] I granted `USE CATALOG` and `USE SCHEMA` and re-audited all three levels
- [ ] I ran `SHOW GRANTS` and read the output
- [ ] I granted `SELECT` at the schema and watched a new table inherit it
- [ ] I read the table's owner, transferred it to a group, and took it back
- [ ] I attached a row filter and watched the count drop from 61,699 to 3,856
- [ ] I masked `#ID_RSSD` and saw it return NULL, then -1 after replacing the function
- [ ] I removed both policies and got the full table back
- [ ] I linked a GitLab repository as a Git folder
- [ ] I worked on a branch rather than the default
- [ ] I committed and pushed a file from the Databricks UI
- [ ] I created a storage credential
- [ ] I created an external location and listed its contents
- [ ] I created an external table at that path
- [ ] I recorded `Type` and `Location` for both tables
- [ ] I granted `READ FILES` on the external location and audited it
- [ ] I can state what happens to the data when each table is dropped

---

## Troubleshooting Reference

> **Key Insight:** Storage failures and catalog failures look alike from the error message but have nothing to do with each other. If `LIST` on the path fails, the problem is IAM. If `LIST` works and `SELECT` fails, the problem is a grant.
<!-- source: facts_extracted.md §1 -->

| Issue | Symptom | Solution |
|---|---|---|
| Cannot create a catalog | Permission denied | You lack `CREATE CATALOG` on the metastore. Ask your instructor. |
| A reader cannot read the table | Access denied with `SELECT` granted | Missing `USE CATALOG` or `USE SCHEMA`. |
| Storage credential creation fails | Error on the IAM role | The role ARN is wrong or does not trust Databricks. |
| `LIST` on the S3 path fails | Access denied error | IAM trust or bucket policy, not Unity Catalog. |
| `LIST` says no such file or directory | Missing path, not denied | Expected on an empty prefix. The credential worked; there is simply nothing there yet. |
| External location overlaps another | `INVALID_PARAMETER_VALUE.LOCATION_OVERLAP` | External locations cannot overlap, and that includes nesting inside an existing one. Use a prefix unique to you, outside any existing location. |
| Git push rejected | Authentication failure | The PAT is expired or lacks write scope on the repository. |
| External table creation fails | Path error | The path must sit inside an external location you have permission on. |
| Mask refuses to attach | Type mismatch on `ALTER COLUMN` | The mask function's parameter type must equal the column type — `#ID_RSSD` is `BIGINT`. |
| Row filter "not working" | You still see 61,699 rows | Filters apply to the owner too, so full visibility means the `ALTER` did not take. Audit with `SELECT * FROM eng_<id>.information_schema.row_filters`. |
| Policy function cannot be created | Permission or location error | Filter and mask functions are Unity Catalog objects — create them in your own schema (`eng_<id>.work`), not in `training_nic`. |

---

## Cost Considerations

| Resource | Driver | Control |
|---|---|---|
| Cluster or warehouse | Billed while running | Let it auto-terminate. |
| Table copies | Two full copies of the source table | Small at training scale; drop both at course end. |
| S3 storage | External table files persist after the table is dropped | Delete the prefix explicitly during cleanup. |

**Cleanup:** Keep the catalog, schema and Git folder—Labs 8–12 all build on them. The row filter and mask were already removed in step 13. Drop the `institutions_*` and `branch_counts` tables at the end of the course and remove the S3 prefix.

---

## Knowledge Check

1. Name the three privileges required to read a table and state which is most often missing.
2. What does a storage credential wrap, and what does an external location add on top of it?
3. You drop a managed table and an external table. What happens to the data in each case?
4. A colleague can `LIST` the S3 path but cannot `SELECT` from the external table over it. Which system is refusing, and why are they separate?
5. Why commit pipeline work on a branch rather than the default branch?
6. During a migration, when would you deliberately choose an external table over a managed one?
7. A row filter and a `WHERE` clause can return identical rows. Name two ways they differ in practice.
8. You replaced a mask function with `CREATE OR REPLACE` and every query changed behavior immediately, with no `ALTER TABLE`. What does that tell you about where the policy lives — and who should be allowed to modify that function?

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
