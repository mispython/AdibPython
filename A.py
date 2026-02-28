AZURE DEVOPS IMPLEMENTATION GUIDELINE (SIMPLIFIED)

------------------------------------------------------------
PART 1 – FIRST TIME SETUP (LOCAL MACHINE)

1. Create folder:
   Data_Warehouse/MIS

2. Close any remote connection in VS Code (press Esc).

3. Clone the repository from Azure DevOps to your local machine.

4. If SSL error occurs, run in Git Bash:
   git config --global http.sslbackend schannel

5. If git prompts for name and email, run:
   git config --global user.name "Full Name"
   git config --global user.email "your_email@publicbank.com.my"

6. Login using your IUIA and password.

------------------------------------------------------------
PART 2 – CREATE NEW BRANCH (USING eSMR NUMBER)

1. Go to Azure DevOps → Repos → Branches.
2. Click New Branch.
3. Branch name: 202X_XXXX (your eSMR number)
4. Base on: main
5. Click Create.

In VS Code:
1. Go to Source Control.
2. Click the three dots (…) → Fetch.
3. Click the current branch name at bottom left.
4. Select your new branch.

------------------------------------------------------------
PART 3 – DEVELOPMENT STEPS

1. Copy job from svdwh004 into:
   Data_Warehouse/MIS

2. In VS Code:
   - Go to Source Control.
   - Enter commit message (your eSMR number).
   - Click Commit.
   - Click Sync.

------------------------------------------------------------
PART 4 – CREATE PULL REQUEST (PR)

DEV ENVIRONMENT
1. Go to Azure DevOps → Repos → Pull Requests.
2. Create new Pull Request.
3. From: 202X_XXXX
4. Into: DEV
5. Resolve conflicts if any.
6. Approve and Complete.
7. Make sure "Delete branch after merge" is NOT checked.

UAT ENVIRONMENT
1. Create new Pull Request.
2. From: 202X_XXXX
3. Into: UAT
4. Approve and Complete.
5. Make sure "Delete branch after merge" is NOT checked.

PROD ENVIRONMENT
1. Create new Pull Request.
2. From: 202X_XXXX
3. Into: main
4. Approve and Complete.
5. Make sure "Delete branch after merge" IS checked.

------------------------------------------------------------
PART 5 – PIPELINE & RELEASE

DEV
1. Pull Request will automatically trigger the Pipeline.
2. Pipeline will automatically create a Release.
3. Go to Azure DevOps → Pipelines → Check status.
4. Go to Azure DevOps → Releases → Verify release.
5. Ensure Deployment is SUCCESS.
6. Check SonarQube – Quality Gate must PASS (0 issues).

UAT
1. Pull Request auto triggers Pipeline.
2. Release will be created automatically.
3. Deployment status will show “Waiting for approval”.
4. Raise eJS with required details:
   - Release Number
   - Repo Name
   - Release Pipeline URL
   - Approval flow: TL → SDM → COM → SCOS
   - Implementation Instructions
   - Source Compare
   - Unit Test
   - Super User / Admin ID
   - Rollback & Recovery

PROD
1. Pull Request triggers Pipeline.
2. Release is NOT auto created.
3. Manually create the Release.
4. Deployment will wait for approval.
5. Raise eJS with required details:
   - Release Number
   - Release Definition
   - Pipeline URL
   - Collection Name
   - Project Name
   - Implementation Instructions
   - Source Compare
   - Unit Test
   - Super User / Admin ID
   - Rollback & Recovery

------------------------------------------------------------
OVERALL FLOW

Create Branch
→ Develop & Commit
→ PR to DEV
→ PR to UAT
→ PR to PROD
→ Delete Branch After PROD
