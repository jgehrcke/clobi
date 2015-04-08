Clobi is a job scheduling system supporting Virtual Machines (VMs) in multiple Infrastructure-as-a-Service computing clouds. Currently, the system is using and supporting Nimbus (and it’s prepared for Cumulus, Nimbus’ storage service) and Amazon Web Services (more precisely EC2, SQS, SimpleDB, S3).

[Here](http://gehrcke.de/files/perm/clobi_jobsystem_scheme.jpg) you can find a visualization of the system.

It’s an “elastic” and “scalable” job system that can set up a huge computing resource pool almost instantly. VMs are added to or removed from the pool dynamically; based on need and demand. Jobs are submitted and processed using an asynchronous queueing system. An arbitrary number of clients is allowed to submit jobs to an existing resource pool. The basic reliability is inherited from the reliability of the core messaging components Amazon SQS (for queueing) and Amazon SimpleDB (for bookkeeping): to prevent data from being lost or becoming unavailable, it is stored redundantly and geographically dispersed across multiple data centers.

Furthermore, the components of the job system are highly decoupled, which allows single components to fail or to get re-initialized without affecting the others.

The motivating application for this system is ATLAS Computing (for the ATLAS experiment at LHC, CERN (Geneva)): a common ATLAS Computing application (the so-called “full chain”) was run during the tests.

But: the basic system is totally generic and can be used in any case whenever it’s convenient to distribute jobs among different clouds. This is always the case when one tries to satisfy the basic computing power needs for a low price by -- for instance -- operating an own Nimbus cloud, but wants to able to instantly balance out peaks of desired computing power by simply adding Amazon’s EC2 to the resource pool for a certain amount of time. By using Clobi, combining different clouds to one big resource pool becomes very easy.

This project is a result of my [Google Summer of Code 2009](http://gehrcke.de/gsoc)