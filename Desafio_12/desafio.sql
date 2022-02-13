		SELECT [CountryRegion].Name, [Currency].Name, AVG(CurrencyRate.AverageRate) as currency_rate, AVG([SalesTaxRate].TaxRate) as average_taxRate
			  FROM [Person].[CountryRegion]
			  JOIN [Person].[StateProvince]
			    ON [CountryRegion].CountryRegionCode = [StateProvince].CountryRegionCode
			  JOIN [Sales].[CountryRegionCurrency]
			    ON [CountryRegionCurrency].CountryRegionCode = [CountryRegion].CountryRegionCode
			  JOIN [Sales].[SalesTaxRate]
			    ON [SalesTaxRate].StateProvinceID = [StateProvince].StateProvinceID
		      JOIN [Sales].[CurrencyRate]
			    ON [SalesTaxRate].SalesTaxRateID = [CurrencyRate].CurrencyRateID
			  JOIN [Sales].[Currency]
			    ON [Currency].CurrencyCode = [CountryRegionCurrency].CurrencyCode
		  GROUP BY [CountryRegion].Name, [Currency].Name

		
