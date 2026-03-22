def get_customer_rules():
    return [
        # Identificadores y Fechas
        {"name": "valid_user_id", "constraint": "user_id IS NOT NULL", "tag": "customers"},
        {"name": "valid_update_date", "constraint": "user_updated_at IS NOT NULL", "tag": "customers"},
        {"name": "valid_registration_date", "constraint": "registration_date IS NOT NULL", "tag": "customers"},
        
        # Perfil Demográfico
        {"name": "valid_age", "constraint": "age >= 18 AND age <= 100", "tag": "customers"},
        {"name": "valid_gender", "constraint": "gender IN ('M', 'F', 'O')", "tag": "customers"},
        {"name": "valid_country", "constraint": "length(country) = 2", "tag": "customers"},
        
        # Atributos de Negocio eCommerce
        {"name": "valid_user_type", "constraint": "user_type IN ('registered', 'guest', 'loyalty')", "tag": "customers"},
        {"name": "valid_preferred_device", "constraint": "preferred_device IN ('desktop', 'mobile', 'tablet')", "tag": "customers"},
        {"name": "valid_return_rate", "constraint": "return_rate >= 0 AND return_rate <= 1", "tag": "customers"},
        
        # Flags Binarios
        {"name": "valid_app_flag", "constraint": "has_app_installed IN (0, 1)", "tag": "customers"},
        {"name": "valid_email_opt", "constraint": "email_opt_in IN (0, 1)", "tag": "customers"}
    ]