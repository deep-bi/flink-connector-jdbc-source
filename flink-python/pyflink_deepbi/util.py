from typing import Dict
from pyflink.java_gateway import get_gateway
from py4j.java_gateway import JavaObject


def dict_to_java_properties(data: Dict[str, str]) -> JavaObject:
    properties = get_gateway().jvm.java.util.Properties()
    for key, value in data.items():
        properties.setProperty(key, value)
    
    return properties