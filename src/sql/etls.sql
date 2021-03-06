-- CLIENTS_RESUME_DROP
drop table if exists etl_clients_resume;

-- CLIENTS_RESUME_CREATE
CREATE TABLE public.etl_clients_resume (
	"timestamp" timestamptz NULL,
	order_date date NULL,
	customer_city varchar NULL,
	nombre_cliente text NULL,
	cantidad_productos_vendidos int8 NULL,
	montos_vendidos float8 null,
	primary key (order_date, customer_city, nombre_cliente)
);

-- CLIENTS_RESUME_INSERT
insert into public.etl_clients_resume
select t.* from (
	select
		now() as timestamp,
		case when o.order_date is null then '1900-01-01'::date else o.order_date end as order_date,
		case when cus.customer_city is null then 'NULL' else cus.customer_city end as customer_city,
		concat(cus.customer_fname, ' ', cus.customer_lname) as nombre_cliente,
		case when sum(oi.order_item_quantity) is null then 0 else sum(oi.order_item_quantity) end as cantidad_productos_vendidos,
		case when sum(oi.order_item_subtotal) is null then 0 else sum(oi.order_item_subtotal) end as montos_vendidos
	from customer cus
		Left join orders o on o.order_customer_id = cus.customer_id
		Left join order_items oi on oi.order_item_order_id = o.order_id
	group by o.order_date, cus.customer_city, concat(cus.customer_fname, ' ', cus.customer_lname)
	order by o.order_date asc
) as t
	left join public.etl_clients_resume e on e.order_date = t.order_date and e.customer_city = t.customer_city and e.nombre_cliente = t.nombre_cliente
where e.order_date is null and e.customer_city is null and e.nombre_cliente is null;

-- CLIENTS_RESUME_QUERY_PER_NAME
SELECT order_date, cantidad_productos_vendidos, montos_vendidos
FROM public.etl_clients_resume
WHERE nombre_cliente = '{client_name}';

-- CLIENTS_RESUME_QUERY_PER_NAME_DATE
SELECT order_date, cantidad_productos_vendidos, montos_vendidos
FROM public.etl_clients_resume
WHERE nombre_cliente = '{client_name}' AND order_date BETWEEN '{start_date}' AND '{end_date}';

-- CLIENTS_RESUME_QUERY_PER_DATE
SELECT *
FROM public.etl_clients_resume
WHERE order_date BETWEEN '{start_date}' AND '{end_date}';

-- RECREATE_DATE_DEPARTMENT_CATEGORY_PRODUCT_RESUME
DROP TABLE IF EXISTS public.date_department_category_product_resume;
CREATE TABLE public.date_department_category_product_resume as
SELECT
	EXTRACT(YEAR FROM ord.order_date::DATE) AS order_year,
	EXTRACT(MONTH FROM ord.order_date::DATE) AS order_month,
	de.department_name,
	ca.category_name,
	CONCAT('(', oi.order_item_product_id, ') ', pr.product_name) AS product,
	SUM(oi.order_item_quantity) AS quantity,
	ROUND(SUM(oi.order_item_subtotal)::NUMERIC, 0) AS subtotal
FROM public.order_items oi
	LEFT JOIN public.products pr ON oi.order_item_product_id = pr.product_id
	LEFT JOIN public.orders ord ON oi.order_item_order_id = ord.order_id
	LEFT JOIN public.categories ca ON pr.product_category_id = ca.category_id
	LEFT JOIN public.departments de ON ca.category_department_id = de.department_id
WHERE pr.product_id IS NOT NULL
GROUP BY 1, 2, 3, 4, 5
ORDER BY 7 DESC, 6 DESC;