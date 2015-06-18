# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

__version__ = "0.1.5"

import os
import sys
import time
import binascii
import multiprocessing as mp

PY2 = sys.version_info[0] == 2

iteritems = (lambda d: d.iteritems()) if PY2 else (lambda d: d.items())

DEFAULT_MAX_PROCS = mp.cpu_count()
DEFAULT_POLL_INTERVAL = 0.001
MAX_POLL_INTERVAL = 3


class PBJobManager(object):

    def __init__(
            self,
            max_procs=DEFAULT_MAX_PROCS,
            verbose=False,
            job_timeout=None):
        self._verbose = verbose
        self._job_timeout = job_timeout
        self.max_procs = max(1, int(max_procs))

        self._jobs = {}

        # Since job_ids are handed out linearly by the manager,
        # we can safely assume that there are no circular dependencies
        self._deps = {}

        self._start_times = {}
        self._futures = {}
        self._done = {}
        self._failed = {}

        self._poll_interval = DEFAULT_POLL_INTERVAL
        # plumbum is imported here so we can to run
        # setup.py without any dependencies
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
            timeout_exceeded = (
                self._job_timeout and
                (time.time() - self._start_times[job_id]) >= self._job_timeout
            )
            job_is_running = exit_code is None and not timeout_exceeded
            if job_is_running:
                continue

            if self._verbose:
                if timeout_exceeded:
                    print("aborting", job_id)
                else:
                    print("finished", job_id)

            # Yeah, yeah, dangerous modification during iteration, but
            # we're finished iterating by now and exit immediatly, so :P
            del self._futures[job_id]
            self._done[job_id] = job_future

            if timeout_exceeded:
                try:
                    job_future.proc.kill()
                except OSError:
                    # job may have finshed after all
                    pass

            # poll may say we are done, but we still need to call wait()
            # so that file handles of popen get closed
            try:
                job_future.wait()
            except (OSError, self.pb.ProcessExecutionError) as err:
                self._failed[job_id] = (job_future, err)

            return

    def _increase_poll_interval(self):
        self._poll_interval = min(
            2 * self._poll_interval,
            MAX_POLL_INTERVAL
        )

    def _wait_on_running(self, max_procs):
        assert max_procs >= 0

        self._poll_interval = DEFAULT_POLL_INTERVAL
        while True:
            # first check if any jobs are done
            self._postproc_done_futures()

            next_job_id = self._get_next_job()
            if self._jobs and next_job_id is None:
                time.sleep(self._poll_interval)
                self._increase_poll_interval()
                continue

            if len(self._futures) <= max_procs:
                return

            time.sleep(self._poll_interval)
            self._increase_poll_interval()

    def dispatch(self):
        self._wait_on_running(self.max_procs)

        job_id = self._get_next_job()

        if job_id is None:
            return

        if self._verbose:
            print("starting", job_id)

        job = self._jobs.pop(job_id)
        job_future = job & self.pb.BG
        self._start_times[job_id] = time.time()
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
