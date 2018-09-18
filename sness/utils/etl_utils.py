from unicodedata import normalize, category

def remove_accents(str_input):
    """
    Remove accents of the string
    """
    if not str_input:
        return ''

    return ''.join((c for c in normalize('NFD', str_input) if category(c) != 'Mn'))


remove_accents_udf = udf(remove_accents)

