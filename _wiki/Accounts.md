# Required logins

Before being able to contribute to the project, access to the following resources /domains are required:

## Accounts

### **Production account**:

The production domain provides access to the production cluster and email. Machines on this domain are locked down and a help desk request must be logged to install new software.

The format for user names is [lastname][initial]_<lastname><initial>@cwglobal.local</initial></lastname>_

### **Non-prod account**:

The non-prod domain provides access to the development/CI cluster, and the pre-prod cluster. Power user access should be available and software can be installed as required. Typically space on the machines is limited and software should be installed on the D drive.

The format for usernames is _npd[initial][lastname]<initial><lastname>@nonprod.local</lastname></initial>_

### MSDN Licence

New users will require an [MSDN licence](https://my.visualstudio.com/subscriptions), these can be requested from the [help desk](https://enstar.service-now.com/). Once received, the MSDN licence should be linked to either a prod or non-prod account.  This can be done by visiting the [MDSN site](https://my.visualstudio.com/subscriptions), creating an (MSDN) account with the Enstar user and supplying the licence code that is received from the help desk.

## _Access_

### Edge Anywhere

Provides access to the production environment.

[https://edgeanywhere-uk.enstargroup.com](https://edgeanywhere-uk.enstargroup.com)

[https://edgeanywhere-us.enstargroup.com](https://edgeanywhere-us.enstargroup.com)

### DevAnywhere

Provides access to the non-production environment.

[https://devanywhere.enstargroup.com](https://devanywhere.enstargroup.com)

## Visual Studio Team Services (VSTS)

Provides access to the [backlog](https://enstargroup.visualstudio.com/GlobalDataHub/_backlogs) and [source code](https://enstargroup.visualstudio.com/_git/GlobalDataHub). Access to VSTS can be granted by an administrator.  Currently Andrey or Neil should be able to grant this access.

Prior to accessing VSTS a production or non-production account is required.  Users will also require an MSDN licence and link the licence to one of those accounts.

 To access the source via a git client alternative login credentials should be set up, this can be done via [user preferences](https://enstargroup.visualstudio.com/_details/security/altcreds).