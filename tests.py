from __future__ import print_function
from __future__ import unicode_literals

import time
import pytest

import plumbum as pb

from pb_job_manager import PBJobManager


@pytest.fixture
def manager():
    return PBJobManager(max_procs=4, verbose=True)


def test_one_job(manager, tmpdir):
    tmp_fn = tmpdir.join("one_job.dat")
    job_cmd = pb.cmd.head["-c", "100", "/dev/urandom"] > tmp_fn.strpath
    manager.add_job(job_cmd)
    assert not tmp_fn.check()
    manager.run()
    assert tmp_fn.check()
    with tmp_fn.open('rb') as fh:
        assert len(fh.read()) == 100


def test_dependant_jobs(manager, tmpdir):
    tmp_fn = tmpdir.join("dependant_jobs.dat")
    job_id = manager.add_job([
        pb.cmd.head["-c", "100", "/dev/urandom"] > tmp_fn.strpath,
        pb.cmd.wc["-c", tmp_fn.strpath],
    ])
    manager.run()
    assert manager[job_id].stdout.startswith("100 ")


# def test_job_cb_chaining(manager):
#     def child_cb():
#         parent_id = manager._deps[child_id]
#         return [
#             pb.cmd.head["-c", ord(char_byte), "/dev/urandom"]
#             for char_byte in manager[parent_id].stdout
#         ]

#     def parent_cb():
#         return [pb.cmd.head["-c", "10", "/dev/urandom"], child_cb]

#     child_id = manager.add_job(parent_cb)
#     manager.run()
#     assert len(manager._done) == 11
