import pandas as pd


def getConfig(configFilename: str = 'config.xlsx') -> dict:
    """Get the application config from excel file

    Args:
        configFilename (str, optional): [description]. Defaults to 'config.xlsx'.

    Returns:
        dict: the application config as dictionary
    """    
    df = pd.read_excel(configFilename, header=None, index_col=0)
    configDict = df[1].to_dict()
    return configDict
