def get_label_rules():
    return [
        # Identificador de sesión para el JOIN posterior
        {"name": "valid_lbl_session_id", "constraint": "session_id IS NOT NULL", "tag": "labels"},
        
        # Variable objetivo del modelo (Abandono)
        {"name": "valid_will_abandon", "constraint": "will_abandon IS NOT NULL AND will_abandon IN (0, 1)", "tag": "labels"},
        
        # Integridad temporal
        {"name": "valid_label_date", "constraint": "label_available_date IS NOT NULL", "tag": "labels"}
    ]