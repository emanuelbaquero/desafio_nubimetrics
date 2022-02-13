    SELECT [StateProvince].CountryRegionCode as country_region_code, AVG([SalesTaxRate].TaxRate) AS average_taxRate
      FROM [Sales].[SalesTaxRate]
      JOIN [Person].[StateProvince]
        ON [SalesTaxRate].StateProvinceID = [StateProvince].StateProvinceID
  GROUP BY [StateProvince].CountryRegionCode