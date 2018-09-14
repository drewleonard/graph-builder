from flask import Flask, url_for
import networkx as nx
from networkx.drawing.nx_agraph import write_dot
import pydot
import graph_builder_utilities as cu

app = Flask(__name__)


@app.route('/graph-builder/<user>')
def user(user):
    """Makes user's connection graph and draws to server.
	
	Parameters
	----------
	user : string
	    User id of user to connect.
	
	Returns
	-------
	svg
	    Opened connection graph svg file
	"""

    g = cu.make(int(user))

    nx.nx_pydot.write_dot(g, '{}.dot'.format(user))
    (gdot, ) = pydot.graph_from_dot_file('{}.dot'.format(user))
    gdot.write_svg('{}.svg'.format(user))

    with open('{}.svg'.format(user), 'r') as f:
        svg = f.read()

    return svg


if __name__ == '__main__':
    app.run()
