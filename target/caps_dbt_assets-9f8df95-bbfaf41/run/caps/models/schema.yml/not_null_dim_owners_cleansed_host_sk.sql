select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select host_sk
from AIRBNB.DEV.dim_owners_cleansed
where host_sk is null



      
    ) dbt_internal_test