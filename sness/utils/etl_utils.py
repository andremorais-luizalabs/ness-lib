from unicodedata import normalize, category
from pyspark.sql.functions import *
def remove_accents(str_input):
    """
    Remove accents of the string
    """
    if not str_input:
        return ''

    return ''.join((c for c in normalize('NFD', str_input) if category(c) != 'Mn'))



