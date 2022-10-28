# Copyright Modal Labs 2022
from __future__ import annotations   # for generic Futures
import concurrent.futures
import sys
from os.path import basename
from urllib.parse import urlparse


_ipython_support = True
try:
    from IPython import get_ipython
    from IPython.display import display, Javascript
except ImportError:
    _ipython_support = False


def is_notebook(stdout=None):
    if stdout is None:
        stdout = sys.stdout
    try:
        import ipykernel.iostream

        return isinstance(stdout, ipykernel.iostream.OutStream)
    except ImportError:
        return False

def get_notebook_name() -> concurrent.futures.Future[str]:
    """Gets a future that eventually contains the name of the current notebook

    Hacky thing inspired by the `ipyparams` package. Injects some js that sends
     data back over a comm to the kernel with information about the current
     filename. Feel free to replace if you find a better (non-frontend) way

     Sadly this seems to not be very useful for us due to: https://github.com/ipython/ipykernel/issues/65
     """
    if not _ipython_support:
        raise Exception("Unexpected: Not in a notebook context")

    notebook_name_future = concurrent.futures.Future()
    comm_manager = get_ipython().kernel.comm_manager

    def update_params(url):
        parsed = urlparse(url)
        notebook_name_future.set_result(basename(parsed.path))

    def target_func(comm, open_msg):
        @comm.on_msg
        def _recv(msg):
            for k, v in msg['content']['data'].items():
                if k == 'notebook_browser_url':
                    update_params(v)

        comm_manager.unregister_target('url_target', target_func)

    comm_manager.register_target('url_target', target_func)
    display(Javascript("""
const comm = Jupyter.notebook.kernel.comm_manager.new_comm('url_target');
comm.send({'notebook_browser_url': window.location.href});
    """))
    return notebook_name_future
