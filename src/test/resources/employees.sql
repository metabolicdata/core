select employees.name as employee_name, age, companies.name as company_name
    from employees join companies
        ON employees.company = companies.id
where
  age
  <
  40