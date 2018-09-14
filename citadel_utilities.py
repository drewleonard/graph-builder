from sqlalchemy import create_engine
import pandas as pd
import networkx as nx


def get_engine():
    """Gets new SQLAlchemy engine object for Snowflake connection.
    Blank for GitHub upload.
    
    Returns
    -------
    SQLAlchemy engine object
        Connection for Snowflake queries.
    """
    return create_engine()


def format_list(l):
    """Format list for SQL 'WHERE IN' clause.
    
    Parameters
    ----------
    l : list of generics
        List to format
    
    Returns
    -------
    string
        Generics formatted for SQL 'WHERE IN' clause.
    """
    l = ['{}'.format(e) for e in l]
    l = ",".join("'{}'".format(e) for e in l)

    return l


def get_connectors(queue, query):
    """Gets queued users' connectors (e.g., devices, image hashes) from user ids.
    
    Parameters
    ----------
    queue : list of generics
        List of queued user's ids.
    query : string
        Query with formatted variable for queued users' ids.
    
    Returns
    -------
    Pandas DataFrame
        Queued users and their connectors.
    """
    users = format_list(queue)
    query = query.format(USERS=users)
    df = pd.read_sql(query, engine)
    df['user'] = df["user"].astype('int')

    return df


def get_connections(query, connectors, connector_type):
    """Gets new connections (users) from queued users' and their connectors.
    
    Parameters
    ----------
    query : string
        Query with formatted variable for connector ids.
    connectors : Pandas DataFrame
        Queued users and their connectors.
    connector_type : string
        Type of connector.
    
    Returns
    -------
    dictionary
        Connectors and all connected users.
    """
    connections = {}
    if connectors.empty:
        return connections

    connectors = list(connectors[connector_type])
    connectors = format_list(connectors)
    query = query.format(CONNECTORS=connectors)
    df = pd.read_sql(query, engine)

    for i, row in df.iterrows():

        connector = row[connector_type]
        user = row["user"]

        if connector not in connections:
            connections[connector] = []

        connections[connector].append(user)

    return connections


def connect_user(connector_type, user, connectors, connections, color):
    """Connects queued user and connections through connectors.
    
    Parameters
    ----------
    connector_type : string
        Type of connector.
    user : int
        User id.
    connectors : Pandas DataFrame
        Queued users and their connectors.
    connections : dictionary
        Connectors and all connected users.
    color : string
        Color of graph edge for connection.
    """

    # Subset df for user
    connectors = connectors[connectors[connector_type].notnull()]
    connectors = connectors[connectors["user"].astype('int') == user]
    connectors = connectors[connectors[connector_type].isin(connections)]

    # Iterate over user's connectors (e.g., devices)
    for connector in connectors[connector_type]:

        # Get list of users connected to connector
        # and filter out current user
        connected_users = [
            connected_user for connected_user in connections[connector]
            if connected_user != user
        ]

        # Queue up users without nodes (i.e., unvisited)
        next_queue.extend([
            connected_user for connected_user in connected_users
            if not graph.has_node(connected_user)
        ])

        # Make nodes for connected users without nodes
        [
            graph.add_node(connected_user) for connected_user in [
                connected_user for connected_user in connected_users
                if not graph.has_node(connected_user)
            ]
        ]

        # Filter list for unconnected users
        unconnected_users = [
            connected_user for connected_user in connected_users
            if not graph.has_edge(user, connected_user, key=connector)
        ]

        # Add edges between user to connected users
        [
            graph.add_edge(
                user,
                unconnected_user,
                key=connector,
                label=connector[:3],
                color=color) for unconnected_user in unconnected_users
        ]


def make(user):
    """Makes user's connection graph.
    
    Parameters
    ----------
    user : int
        User id of user to connect.
    
    Returns
    -------
    NetworkX MultiGraph object
        User's connection graph.
    """

    # Initialize SQL engine
    global engine
    engine = get_engine()

    # Initalize graph
    global graph
    graph = nx.MultiGraph()
    graph.add_node(user)

    # Initialize queues
    queue = [user]
    global next_queue

    accounts = 0
    layers = 0

    # Make algorithm
    while queue:

        next_queue = []

        connectors_device = get_connectors(queue, device_connector_query)
        connections_device = get_connections(device_connection_query,
                                             connectors_device, "device")

        connectors_phone = get_connectors(queue, phone_connector_query)
        connections_phone = get_connections(phone_connection_query,
                                            connectors_phone, "phone")

        connectors_truyou = get_connectors(queue, truyou_connector_query)
        connections_truyou = get_connections(truyou_connection_query,
                                             connectors_truyou, "truyou")

        for user in queue:

            # Connect users through devices
            connect_user("device", user, connectors_device, connections_device,
                         "blue")

            # Connect users through phones
            connect_user("phone", user, connectors_phone, connections_phone,
                         "red")

            # Connect users through truyou
            connect_user("truyou", user, connectors_truyou, connections_truyou,
                         "green")

        queue = next_queue

        accounts += len(queue)
        layers += 1

    print "NUMBER OF CONNECTED ACCOUNTS:{}".format(accounts)
    print "NUMBER OF LAYERS:{}".format(layers)
    return graph
    