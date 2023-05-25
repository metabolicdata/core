WITH deleted_shifts_events AS (
	SELECT id,
		cast(company_id as int) as company_id,
		timestamp,
		event_name,
		cast(event_id as int) as shift_id,
		access_id

	FROM clean_event_types
	WHERE clean_event_type like '%AttendanceShiftDeleted%'
		AND event_name = 'Attendance::Shift'
	ORDER BY timestamp desc

)

SELECT 
    de.company_id,
    de.shift_id,
    de.access_id,
    ras.day,
	ras.clock_in,
	ras.clock_out,
	ras.attendance_period_id,
	ras.created_at,
	ras.updated_at,
	ras.observations,
	ras.in_location_latitude,
    ras.in_location_longitude,
    ras.in_location_accuracy,
    ras.out_location_latitude,
    ras.out_location_longitude,
    ras.out_location_accuracy,
    ras.date,
    ras.employee_id,
    ras.half_day,
    ras.workable,
    ras.automatic_clock_in,
    ras.automatic_clock_out

FROM deleted_shifts_events de
LEFT JOIN dl_deduped_attendance_shifts ras ON de.shift_id = ras.id
LEFT JOIN clean_accesses ca ON ras.employee_id = ca.employee_id
