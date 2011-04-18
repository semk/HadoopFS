### Usage ###

    from hadoopfs.gen_py.ttypes import Pathname
    from hadoopfs import HDFSClient
    
    # Create HDFS connection
    client = HDFSClient('127.0.0.1', 9000)

    # Define a path
    path = Pathname('/test')
    # Check whether 'path' exists
    print client.exists(path)
    # Close the connection
    client.close()
