"""
Expone la función principal 'get_rules' para el pipeline de Delta Live Tables.
Agrega las reglas de Clientes (customers), Eventos (transactions) y Etiquetas (labels).
"""

from .customers import get_customer_rules
from .transactions import get_transaction_rules
from .labels import get_label_rules

def _get_all_rules_as_list_of_dict():
    """Combina todos los catálogos de reglas en una única lista."""
    all_rules = []
    all_rules.extend(get_customer_rules())
    all_rules.extend(get_transaction_rules())
    all_rules.extend(get_label_rules())
    return all_rules

def get_rules(tag):
    """
    Retorna un diccionario {nombre: restricción_sql} para el decorador @dp.expect_all,
    filtrado por la etiqueta (tag) de la tabla correspondiente.
    """
    return {
        row["name"]: row["constraint"]
        for row in _get_all_rules_as_list_of_dict()
        if row["tag"] == tag
    }