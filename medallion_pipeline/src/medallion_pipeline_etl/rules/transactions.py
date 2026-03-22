def get_transaction_rules():
    return [
        # Identificadores clave
        {"name": "valid_tx_session_id", "constraint": "session_id IS NOT NULL", "tag": "transactions"},
        {"name": "valid_tx_user_id", "constraint": "user_id IS NOT NULL", "tag": "transactions"},
        
        # Valor del carrito y marcas temporales
        {"name": "valid_cart_value", "constraint": "cart_value IS NOT NULL AND cart_value >= 0", "tag": "transactions"},
        {"name": "valid_event_timestamp", "constraint": "event_timestamp IS NOT NULL", "tag": "transactions"},
        
        # Tipos de eventos de navegación permitidos
        {"name": "valid_event_type", 
         "constraint": "event_type IN ('add_to_cart', 'view', 'remove_from_cart', 'checkout', 'click')", 
         "tag": "transactions"},
        
        # Categoría del producto
        {"name": "valid_item_category", "constraint": "item_category IS NOT NULL", "tag": "transactions"}
    ]