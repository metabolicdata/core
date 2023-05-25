 SELECT
     factorial_yaml_binary_to_json_string(unbase64(metadata)) as metadata
 FROM (
          SELECT
                 get_json_object(value, '$.EVENT_TYPE')       as event_type,
                 get_json_object(value, '$.STREAM')           as stream,
                 get_json_object(value, '$.EVENT_CREATED_AT') as created_at,
                 get_json_object(value, '$.YAML_METADATA')    as metadata,
                 get_json_object(value, '$.YAML_DATA')        as data
          FROM factorial_events
      )