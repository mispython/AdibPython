New Fixes and Implementations Guideline

FIRST STEPS -> CREATE PULL REQUEST -> RUN PIPELINE
First steps
1.	Ensure a folder name Data_Warehouse/MIS created in your local machine.
2.	Ensure you have closed the remote connection in Visual Studio Code (Esc key).
3.	Clone the git repository from Azure DevOps to your local machine.
4.	If Error, try git config –global http.sslbackend schannel in Git Bash.
5.	If Error prompts user.name and user.email, do as below in Git Bash:
a.	git config –global user.name “full name”
b.	git config –global user.email “adid@publicbank.com.my”
6.	Then enter your IUIA and password. Try to clone again from Azure DevOps.
7.	Create a new branch. Name branch using eSMR number, e.g. 202X_XXXX. Below is the step to create new branch.
a.	Azure DevOps. Go to your repo, then click branches in the sidebar. 
Click New branch. Input the eSMR number in the Name field and select MAIN from the Based on dropdown. Create.
8.	In VS Code, go to the Source Control panel, click on the 3 dots aligned with ‘changes’
a.	Click Fetch to fetch your new branch
b.	Go to the bottom left corner, click on the current Git branch name, e.g. main
c.	A list of branch will pop up from the top menu, choose your branch.
9.	Implement any development changes in this branch.
a.	Paste the job from svdwh004 to the Data_Warehouse/MIS folder in your local machine.
b.	In VS Code, go to the Source Control panel in the left bar.
c.	Type your commit message which should be your eSMR number, e.g. 202X_XXXX.
d.	Click Commit to commit your changes.
e.	Click Sync

Creating a Pull Request (DEV & UAT)
DEV
1.	In Azure DevOps, go to Repos, go to Pull Requests.
2.	Create new pull request.
3.	Ensure that your pull request is from eSMR branch, 202X_XXXX into DEV branch. Click create.
4.	If merge conflicts are detected, resolve. Else, Approve, then Complete. 
5.	Approve and complete can be done by developers itself.
6.	Ensure that the checkbox for Delete 202X_XXXX after merging is NOT checked. Please UNCHECK that option before proceeding.
UAT
1.	In Azure DevOps, go to Repos, go to Pull Requests.
2.	Create new pull request.
3.	Ensure that your pull request is from eSMR branch, 202X_XXXX into UAT branch. Click create.
4.	If merge conflicts are detected, resolve. Else, Approve, then Complete. 
5.	Approve and complete can be done by developers itself.

6.	Ensure that the checkbox for Delete 202X_XXXX after merging is NOT checked. Please UNCHECK that option before proceeding.
Prod
1.	In Azure DevOps, go to Repos, go to Pull Requests.
2.	Create new pull request.
3.	Ensure that your pull request is from eSMR branch, 202X_XXXX into main branch. Click create.
4.	If merge conflicts are detected, resolve. Else, Approve, then Complete. 
5.	Approve and complete can be done by developers itself.
6.	Ensure that the checkbox for Delete 202X_XXXX after merging is CHECKED. After implementation, eSMR branch is no longer necessary.
Pipeline & Release (DEV & UAT)
DEV
1.	Pull Request will auto trigger the Pipeline.
2.	Pipeline will auto create a release.
3.	In Azure DevOps, go to Releases, you should be able to view your release.
4.	In Azure DevOps, go to Pipelines. Click All and select the repo you are working on.
5.	Take note of the Release Number, e.g. #20250623.3 Merged PR 1: 202X_XXXX.
6.	Deployment should succeed for DEV pipeline.
7.	Go to SonaQube, ensure the quality gate passed and 0 issues detected, else resolve them.
UAT
1.	Pull Request will auto trigger the Pipeline.
2.	Pipeline will auto create a release.
3.	In Azure DevOps, go to Releases, you should be able to view your release.
4.	In Azure DevOps, go to Pipelines. Click All and select the repo you are working on.
5.	Take note of the Release Number, e.g. #20250623.3 Merged PR 1: 202X_XXXX.
6.	Deployment should indicate that it is waiting for team lead’s approval. 
(Pre-deployment approval pending)
7.	Raise eJS. Example is given below.
eJS Ref: A2025-00023510
http://webnotes01.pbb.my/esmr/ejobspec.nsf/0/D957CF1897F05A9E48258CB2002CBABE?OpenDocument 
Prod
1.	Pull Request will auto trigger the Pipeline.
2.	Pipeline will NOT auto create a release. Hence, you need to create on your own.
3.	In Azure DevOps, go to Releases, you should be able to view your release
4.	In Azure DevOps, go to Pipelines. Click All and select the repo you are working on.
5.	Take note of the Release Number, e.g. #20250623.3 Merged PR 1: 202X_XXXX.
6.	Deployment should indicate that it is waiting for team lead’s approval.
7.	Raise eJS. Example is given below (All information is taken from the following eJS.
eJS Ref: 2024-00001420
http://webnotes01.pbb.my/esmr/ejobspec.nsf/0/AFA74C59A06BB1EE48258CB4002023A2?OpenDocument
Sample Job Description:
Azure DevOps release approval
TL -> SDM -> COM ->SCOS
Collection : DWHCollection
Project : DataWarehouse
Release Definition : eBANKING - prd
Release number/ Title : Release -1
https://bgnvmtfa001/tfs/DWHCollection/DataWarehouse/_releaseProgress?_a=release-pipeline-progress&releaseId=23
Please modify the above with your associated repo, Release and Release Pipeline URL.
Main Instructions
☒ Source Compare – ChangeMan
☒ Unit Test
☒ Super User / Admin ID

For how to fill other required information, please see the following section in the provided link. 
-	Impl Instructions
-	Source Compare
-	Super User / Admin ID
-	Unit Test
-	Rollback & Recovery
