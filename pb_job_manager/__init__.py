# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

__version__ = "0.1.3"

import os
import sys
import time
import binascii
import multiprocessing as mp

PY2 = sys.version_info[0] == 2

iteritems = (lambda d: d.iteritems()) if PY2 else (lambda d: d.items())

DEFAULT_MAX_PROCS = mp.cpu_count()
DEFAULT_POLL_INTERVAL = 0.001


class PBJobManager(object):

    def __init__(self, max_procs=DEFAULT_MAX_PROCS, verbose=False):
        self._verbose = verbose
        self.max_procs = max(1, int(max_procs))

        self._jobs = {}

        # Since job_ids are handed out linearly by the manager,
        # we can safely assume that there are no circular dependencies
        self._deps = {}

        self._futures = {}
        self._done = {}

        self._poll_interval = DEFAULT_POLL_INTERVAL
        import plumbum
        self.pb = plumbum

    def mk_job_id(self):
        return binascii.hexlify(os.urandom(8)).decode('ascii')

    def add_job(self, job, dep_job_id=None):
        if isinstance(job, list):
            subjob_iter = iter(job)
            job_id = self.add_job(next(subjob_iter))
            for sub_job in subjob_iter:
                # job_id of the last job is returned
                job_id = self.add_job(sub_job, dep_job_id=job_id)
        else:
            job_id = self.mk_job_id()
            if dep_job_id:
                self._deps[job_id] = dep_job_id
            self._jobs[job_id] = job
        return job_id

    def _get_next_job(self):
        for job_id, job in iteritems(self._jobs):
            dep_job_id = self._deps.get(job_id)
            if dep_job_id and dep_job_id not in self._done:
                # wait for dep job to finish
                continue

            if isinstance(job, self.pb.commands.base.BaseCommand):
                return job_id

            # TODO: see if this breaks with remote commands
            if callable(job):
                self.add_job(job())
                del self._jobs[job_id]
                return self._get_next_job()

    def _postproc_done_futures(self):
        for job_id, job_future in iteritems(self._futures):
            exit_code = job_future.proc.poll()
            if exit_code is None:
                # still running
                continue

            if self._verbose:
                print("finished", job_id)

            # Yeah, yeah, dangerous modification during iteration,
            # but we're finished iterating by now, so :P
            del self._futures[job_id]
            self._done[job_id] = job_future
            # poll says we are done, but we still need to call wait()
            # so that file handles of popen get closed
            job_future.wait()
            return

    def _wait_on_running(self, max_procs):
        assert max_procs >= 0

        self._poll_interval = DEFAULT_POLL_INTERVAL
        while True:
            # first check if any jobs are done
            self._postproc_done_futures()

            next_job_id = self._get_next_job()
            if self._jobs and next_job_id is None:
                time.sleep(self._poll_interval)
                self._poll_interval *= 2
                continue

            if len(self._futures) <= max_procs:
                return

            time.sleep(self._poll_interval)
            self._poll_interval *= 2

    def dispatch(self):
        self._wait_on_running(self.max_procs)

        job_id = self._get_next_job()

        if job_id is None:
            return

        if self._verbose:
            print("starting", job_id)

        job = self._jobs.pop(job_id)
        job_future = job & self.pb.BG
        self._futures[job_id] = job_future
        return job_id

    def wait(self):
        self._wait_on_running(max_procs=0)

    def run(self):
        while len(self._jobs) > 0:
            self.dispatch()
        self.wait()

    def __getitem__(self, job_id):
        return self._done[job_id]
