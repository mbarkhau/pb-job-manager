===================
Plumbum Job Manager
===================

----------------------------------------------------
A utility class to run plumbum commands concurrently
----------------------------------------------------

.. image:: https://secure.travis-ci.org/mbarkhau/pb-job-manager.png
   :target: http://travis-ci.org/mbarkhau/pb-job-manager
   :width: 90
   :height: 20


Fire and forget Multiple Commands
=================================

::

	import plumbum as pb
	from pb_job_manager import PBJobManager
	pbjm = PBJobManager(max_procs=4)
	pbjm.add_job(pb.cmd.grep["foo", "input.txt"] | pb.cmd.sort > "foo.txt")
	pbjm.add_job(pb.cmd.grep["bar", "input.txt"] | pb.cmd.sort > "bar.txt")
	pbjm.add_job(pb.cmd.grep["baz", "input.txt"] | pb.cmd.sort > "baz.txt")
	pbjb.run()   # run until all jobs are finished


Creating Jobs with Callbacks
============================

::
	TODO: Example

Jobs with dependencies
======================

::
	TODO: Example


Result Iteration
================

::
	TODO: Example
