---
description: Placeholders are dynamic variables that alter the execution of the model.
---

# 👣 Placeholders

## Defaults

Under the df prefix, these placeholders help deal with time variants.

### Now

* **`df.now`**: Current date time in UTC timezone.

### Daily

* **`df.today`**: Current date in UTC timezone.
* **`df.start_of_today`**: 00:00:00 of current date in UTC timezone.
* **`df.end_of_today`**: 23:59:59 of current date in UTC timezone.



* **`df.tomorrow`**: Current date plus 1.
* **`df.start_of_tomorrow`**: 00:00:00.000 of current date plus 1 in UTC timezone.
* **`df.end_of_tomorrow`**: 23:59:59.999 of current date plus 1 in UTC timezone.



* **`df.yesterday`**: Current date minus 1.
* **`df.start_of_yesterday`**: 00:00:00.000 of current date minus 1 in UTC timezone.
* **`df.end_of_yesterday`**: 23:59:59.999 of current date minus 1 in UTC timezone.

### Monthly

* **`df.month`**: Current month in two digit format.
* **`df.previous_month`**: Current month minus 1 in two digit format.
* **`df.next_month`**: Current month plus 1 in two digit format.



* **`df.year_month`**: Current month in YYYY-MM format.
* **`df.previous_year_month`**: Current month minus 1 in YYYY-MM format.
* **`df.next_year_month`**: Current month plus 1 in YYYY-MM format.



* **`df.first_day_of_month`**: Date of the first day (01) of current month in YYYY-MM format.
* **`df.previous_first_day_of_month`**: Date of the first day (01) of current month minus 1 in YYYY-MM format.
* **`df.next_first_day_of_month`**: Date of the first day (01) of current month plus 1 in YYYY-MM format.



* **`df.last_day_of_month`:** Date of the last day (28,29,30 or 31) of current month in YYYY-MM format.
* **`df.previous_last_day_of_month`**:  Date of the last day (28,29,30 or 31) of current month minus 1 in YYYY-MM format.
* **`df.next_last_day_of_month`**:  Date of the last day (28,29,30 or 31) of current month plus 1 in YYYY-MM format.



* **`df.start_of_month`**: First day of current month at 00:00:00.000 in UTC timezone.
* **`df.previous_start_of_month`**: First day of current month minus 1 at 00:00:00.000 in UTC timezone.
* **`df.next_start_of_month`:** First day of current month plus 1 at 00:00:00.000 in UTC timezone



* **`df.end_of_month`**: Last day of current month at 23:59:59.999 in UTC timezone.
* **`df.previous_end_of_month`**: Last day (28,29,30 or 31) of current month minus 1 at 23:59:59.999 in UTC timezone
* **`df.next_end_of_month`**: Last day (28,29,30 or 31) of current month plus 1 at 23:59:59.999 in UTC timezone.

### Yearly

* **`df.year`**: Current year in YYYY format.
* **`df.previous_year`**: Current year minus 1 in YYYY format
* **`df.next_year`**: Current year plus 1 in YYYY format



* **`df.first_day_of_year`:** Date of January 1st of current year in UTC timezone.
* **`df.previous_first_day_of_year`**: Date of January 1st of current year minus 1 in UTC timezone.
* **`df.next_first_day_of_year`**: Date of January 1st of current year plus 1 in UTC timezone.



* **`df.last_day_of_year`:** Date of December 31st of current year in UTC timezone.
* **`df.previous_last_day_of_year`**:  Date of December 31st of current year minus 1 in UTC timezone.
* **`df.next_last_day_of_year`**: Date of December 31st of current year plus 1 in UTC timezone.



* **`df.start_of_year`**: Date time of January 1st at 00:00:00.000 of current year in UTC timezone.
* **`df.previous_start_of_year`**: Date time of January 1st at 00:00:00.000 of current year minus 1 in UTC timezone.
* **`df.next_start_of_year`:** Date time of January 1st at 00:00:00.000 of current year plus 1 in UTC timezone.



* **`df.end_of_year`**: Date time of December 31st at 23:59:59.999 of current year in UTC timezone.
* **`df.previous_end_of_year`**:Date time of December 31st at 23:59:59.999 of current year minus 1 in UTC timezone.
* **`df.next_end_of_year`**: Date time of December 31st at 23:59:59.999 of current year plus 1 in UTC timezone.





\


\
