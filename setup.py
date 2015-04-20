from setuptools import setup
from os.path import join, dirname
from pb_job_manager import __version__


def read(fname):
    with open(join(dirname(__file__), fname), 'r') as f:
        return f.read()


setup(
    name='pb_job_manager',
    version=__version__,
    description='A utility class to run plumbum commands in parallel',
    long_description=read('README.rst'),
    author='Manuel Barkhau',
    author_email='mbarkhau@gmail.com',
    url='https://github.com/mbarkhau/pb-job-manager/',
    license="BSD License",
    packages=['pb_job_manager'],
    install_requires=['plumbum>=1.4'],
    extras_require={'dev': ["pytest", "wheel"]},
    keywords="plumbum, shell, popen, process, subprocess, multiprocess",
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        'Topic :: Utilities',
    ],
)
