import pyalex

pyalex.config.email = "namemat@gmail.com"

universities = {
    # West Pomeranian University of Technology in Szczecin
    'PL_ZUT': 'https://ror.org/0596m7f19',
    # Burgas Free University
    'BG_BFU': 'https://ror.org/02ek1bx64',
    # University of Patras
    'GR_UOP': 'https://ror.org/017wvtq80',
    # University of Dubrovnik
    'HR_UNIDU': 'https://ror.org/05yptqp13',
    # University EMUNI
    'SL_EMUNI': 'https://ror.org/03761pf32',
    # University of Sassari
    'IT_UNISS': 'https://ror.org/01bnjbv91',
    # University of the Antilles
    'FR_UAG': 'https://ror.org/02ryfmr77',
    # University of the Azores
    'PT_UAC': 'https://ror.org/04276xd64',
    # University of the Balearic Islands
    'ES_UIB': 'https://ror.org/03e10x626',
    # University Le Havre Normandie
    'FR_ULHN': 'https://ror.org/05v509s40',
    # University of the Faroe Islands
    'FO_UF': 'https://ror.org/05mwmd090',
    # Stralsund University of Applied Sciences
    'DE_HOCHSTRALSUND': 'https://ror.org/04g99jx54',
    # Åland University of Applied Sciences
    'FI_AUAS': 'https://ror.org/05mknbx32',
}

res = pyalex.Institutions()[universities['FI_AUAS']]

print(res)
