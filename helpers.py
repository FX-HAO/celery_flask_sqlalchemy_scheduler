from sqlalchemy.inspection import inspect

from app import db


def get_class_by_modelname(modelname):
    for cls in db.Model._decl_class_registry.values():
        if cls.__class__.__name__ == modelname:
            return cls

def get_primary_key_name(cls):
    return inspect(cls).primary_key[0].name

def get_primary_key_value(self):
    return getattr(self, get_primary_key_name(self.__class__))
