select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    host_id as unique_field,
    count(*) as n_records

from AIRBNB.DEV.dim_owners_cleansed
where host_id is not null
group by host_id
having count(*) > 1



      
    ) dbt_internal_test