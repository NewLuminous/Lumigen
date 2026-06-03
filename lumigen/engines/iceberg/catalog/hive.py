from __future__ import annotations
import os
import time
import shutil
import tempfile
import uuid
import xml.etree.ElementTree as ET
from xml.dom import minidom
from pyiceberg.catalog.hive import HiveCatalog as BaseHiveCatalog


class HiveCatalog(BaseHiveCatalog):
    BASE_CONF_DIR = os.path.abspath(os.path.join(tempfile.gettempdir(), "pyiceberg_hive_confs"))

    def __init__(self, name: str, **properties: str):
        core_conf = {}
        hdfs_conf = {}
        yarn_conf = {}
        kms_conf = {}
        for prop, val in properties.items():
            if prop.startswith("dfs."):
                hdfs_conf[prop] = val
            elif prop.startswith("yarn."):
                yarn_conf[prop] = val
            elif prop.startswith("hadoop.kms."):
                kms_conf[prop] = val
            else:
                # Includes Iceberg, S3, and standard Hadoop core properties
                core_conf[prop] = val

        # Create and manage temporary directories on disk
        conf_dir = self._prepare_conf_directory()
        if core_conf:
            self._write_hadoop_xml(core_conf, os.path.join(conf_dir, "core-site.xml"))
        if hdfs_conf:
            self._write_hadoop_xml(hdfs_conf, os.path.join(conf_dir, "hdfs-site.xml"))
        if yarn_conf:
            self._write_hadoop_xml(yarn_conf, os.path.join(conf_dir, "yarn-site.xml"))
        if kms_conf:
            self._write_hadoop_xml(kms_conf, os.path.join(conf_dir, "kms-site.xml"))

        # Inject into CLASSPATH safely, keeping ONLY the single current temp dir
        existing_classpath = os.environ.get("CLASSPATH", "")
        cp_elements = existing_classpath.split(os.pathsep) if existing_classpath else []
        other_dirs = [
            p for p in cp_elements 
            if p and not p.startswith(self.BASE_CONF_DIR)
        ]
        os.environ["CLASSPATH"] = os.pathsep.join([conf_dir] + other_dirs)

        super().__init__(name, **properties)

    def _prepare_conf_directory(self) -> str:
        """
        Deletes all existing catalog conf directories and provisions one fresh directory.
        """
        # Clear out any existing directories to keep the disk 100% clean
        if os.path.exists(self.BASE_CONF_DIR):
            try:
                shutil.rmtree(self.BASE_CONF_DIR, ignore_errors=True)
            except Exception:
                pass
                
        # Provision the single new directory
        current_time_ms = int(time.time() * 1000)
        unique_suffix = uuid.uuid4().hex[:6]
        current_conf_dir = os.path.join(self.BASE_CONF_DIR, f"conf_{current_time_ms}_{unique_suffix}")
        os.makedirs(current_conf_dir, exist_ok=True)
        
        return current_conf_dir

    def _write_hadoop_xml(self, conf_dict: dict[str, str], filepath: str) -> None:
        """
        Takes a dictionary of properties and writes a valid Hadoop XML file.
        """
        configuration = ET.Element("configuration")
        
        for key, value in conf_dict.items():
            property_el = ET.SubElement(configuration, "property")
            name_el = ET.SubElement(property_el, "name")
            name_el.text = str(key)
            value_el = ET.SubElement(property_el, "value")
            value_el.text = str(value)
            
        xml_str = minidom.parseString(ET.tostring(configuration)).toprettyxml(indent="  ")
        
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(xml_str)