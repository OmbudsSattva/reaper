-record(gateway, {machine_id, soil_id, utc_offset, production_soil = true, last_retrieved}).

% Incoming AMQP Messages
-record(register_gateway, {machine_id :: string(), password, soil_id :: string(), utc_offset, production_soil = true}).
-record(unregister_gateway, {machine_id}).
-record(change_gateway_password, {machine_id, password}).
-record(rename_gateway, {machine_id, password, old_machine_id}).
-record(historical_data_request, {machine_id, protocol_version, device_name, property_names, start_time, end_time}).
-record(log_request, {machine_id, protocol_version, device_name, property_names, start_time, end_time}).
-record(live_data_request, {machine_id, protocol_version, device_name, property_names, host="", soil_id="" }).
-record(manage_packages, {machine_id, protocol_version, command, command_id, packages = [], config_xml}).
-record(alerter_config, {machine_id, protocol_version, config_xml}).

-type incoming_amqp() :: #register_gateway{}
                       | #unregister_gateway{}
                       | #change_gateway_password{}
                       | #rename_gateway{}
                       | #historical_data_request{}
                       | #log_request{}
                       | #live_data_request{}
                       | #manage_packages{}
                       | #alerter_config{}.

% Outgoing AMQP Messages (incoming XMPP messages)

-record(property_values, {machine_id, type = report, devices = []}).
-record(manage_packages_response, {machine_id::string(), command, command_id, progress::string(), success::boolean()}).
-record(status_basic, {machine_id, status}).
-record(status_advanced, {machine_id, status, config_timestamp, protocol_version::integer(), packages = []}).
-record(package, {name::string(), version::string(), installed::boolean()}).
-record(crash_report, {machine_id::string(), content::string()}).
-record(alarm_result, {machine_id::string(), content::string()}).
-record(alerter_config_response, {machine_id::string(), content::string()}).

-type outgoing_amqp() :: #property_values{}
                       | #manage_packages_response{}
                       | #status_basic{}
                       | #status_advanced{}
                       | #crash_report{}
                       | #alerter_config_response{}.
