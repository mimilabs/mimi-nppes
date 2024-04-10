# mimi-nppes
nppes data loading

## how frequently the column value changes over months

I combined monthly snapshots and calculated how many unique values (changes) are observed over approximately 4 years.

For example, `other_provider_identifiers` has 1.51, which means that one out of two providers changed their other ID section during that time. 

entity_type | colname | change_freq
-----------| ---------|------------
1 | provider_last_name_legal_name | 1.02036487985885
1 | provider_first_name | 1.0019030883270554
1 | provider_middle_name | 1.0089103695159614
1 | provider_name_prefix_text | 1.005303044006905
1 | provider_name_suffix_text | 1.0005171850615178
1 | provider_credential_text | 1.026809412510399
1 | provider_other_last_name | 1.015514082478768
1 | provider_other_first_name | 1.002788178317557
1 | provider_other_middle_name | 1.0052452487583066
1 | provider_other_name_prefix_text | 1.0044171409855802
1 | provider_other_name_suffix_text | 1.00083857442348
1 | provider_other_credential_text | 1.0129533678756477
1 | provider_other_last_name_type_code | 1.0070406343105673
1 | provider_first_line_business_mailing_address | 1.1433016652813706
1 | provider_second_line_business_mailing_address | 1.0150815517214542
1 | provider_business_mailing_address_city_name | 1.1088372924329621
1 | provider_business_mailing_address_state_name | 1.0482960858730648
1 | provider_business_mailing_address_postal_code | 1.142516514811332
1 | provider_business_mailing_address_country_code_if_outside_us | 1.0001749348146378
1 | provider_business_mailing_address_telephone_number | 1.1089717046238785
1 | provider_business_mailing_address_fax_number | 1.0575890630347304
1 | provider_first_line_business_practice_location_address | 1.14684116915295
1 | provider_second_line_business_practice_location_address | 1.0116090335841168
1 | provider_business_practice_location_address_city_name | 1.1093091102919719
1 | provider_business_practice_location_address_state_name | 1.0456228731224362
1 | provider_business_practice_location_address_postal_code | 1.1450361684904071
1 | provider_business_practice_location_address_country_code_if_outside_us | 1.0001845831181577
1 | provider_business_practice_location_address_telephone_number | 1.1348460521979922
1 | provider_business_practice_location_address_fax_number | 1.0624775208922288
1 | provider_enumeration_date | 1
1 | last_update_date | 1.318849542615054
1 | npi_reactivation_date | 1
1 | provider_gender_code | 1.0002287122802678
1 | is_sole_proprietor | 1.0125483324723672
1 | certification_date | 1.3266407959103492
1 | healthcare_provider_taxonomies | 1.1530906778190582
1 | other_provider_identifiers | 1.5162503756246164
1 | healthcare_provider_taxonomy_groups | 1.015467345347071
1 | _input_file_date | 41.73748073304963
2 | provider_organization_name_legal_business_name | 1.0386651406303677
2 | provider_other_organization_name | 1.0352226478333733
2 | provider_other_organization_name_type_code | 1.007934291220608
2 | provider_first_line_business_mailing_address | 1.0699699540853782
2 | provider_second_line_business_mailing_address | 1.0098005551955276
2 | provider_business_mailing_address_city_name | 1.0360434721196117
2 | provider_business_mailing_address_state_name | 1.0108622171897044
2 | provider_business_mailing_address_postal_code | 1.0662017645123527
2 | provider_business_mailing_address_country_code_if_outside_us | 1.000009300341148
2 | provider_business_mailing_address_telephone_number | 1.0477265178161104
2 | provider_business_mailing_address_fax_number | 1.0244712682413732
2 | provider_first_line_business_practice_location_address | 1.0615386135632687
2 | provider_second_line_business_practice_location_address | 1.005557633201162
2 | provider_business_practice_location_address_city_name | 1.0238826947970985
2 | provider_business_practice_location_address_state_name | 1.002160585502977
2 | provider_business_practice_location_address_postal_code | 1.056384063354108
2 | provider_business_practice_location_address_country_code_if_outside_us | 1.0000139505117223
2 | provider_business_practice_location_address_telephone_number | 1.0379874731563483
2 | provider_business_practice_location_address_fax_number | 1.0197659917213768
2 | provider_enumeration_date | 1
2 | last_update_date | 1.3832362513347443
2 | authorized_official_last_name | 1.0776624987575325
2 | authorized_official_first_name | 1.0773683754687227
2 | authorized_official_middle_name | 1.0360827887880215
2 | authorized_official_title_or_position | 1.070454857227889
2 | authorized_official_telephone_number | 1.0779955672249002
2 | is_organization_subpart | 1.0042919178472396
2 | parent_organization_lbn | 1.0352031285697008
2 | authorized_official_name_prefix_text | 1.0087301701636677
2 | authorized_official_name_suffix_text | 1.0016831474857983
2 | authorized_official_credential_text | 1.0149829725346085
2 | certification_date | 1.4581907696118654
2 | healthcare_provider_taxonomies | 1.0943498552340407
2 | other_provider_identifiers | 1.4949251309434122
2 | healthcare_provider_taxonomy_groups | 1.0263379355075681
2 | _input_file_date | 42.93082057491221

