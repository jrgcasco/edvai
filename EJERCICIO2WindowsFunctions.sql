--AVG
/*1. Obtener el promedio de precios por cada categoría de producto. La cláusula
OVER(PARTITION BY CategoryID) específica que se debe calcular el promedio de
precios por cada valor único de CategoryID en la tabla. */

select c.category_name , p.product_name , p.unit_price , avg(p.unit_price) OVER (partition by c.category_id) as avgpricebycategory
from products p inner join categories c 
on p.category_id = c.category_id 

--2. Obtener el promedio de venta de cada cliente

select AVG(od.unit_price*od.quantity) over (partition by o.customer_id) as avgorderamount, o.order_id,o.customer_id, o.employee_id, o.order_date, o.required_date,o.shipped_date 
from orders o inner join order_details od 
on o.order_id = od.order_id 


/* 3. Obtener el promedio de cantidad de productos vendidos por categoría (product_name,
quantity_per_unit, unit_price, quantity, avgquantity) y ordenarlo por nombre de la
categoría y nombre del producto */

select p.product_name , c.category_name, p.quantity_per_unit,p.unit_price, od.quantity , AVG (od.quantity) over (partition by c.category_name)
from (products p inner join categories c  
on p.category_id = c.category_id) inner join order_details od on od.product_id = p.product_id 
order by c.category_name, p.product_name 


--MIN

/* 4. Selecciona el ID del cliente, la fecha de la orden y la fecha más antigua de la
orden para cada cliente de la tabla 'Orders'*/

select c.customer_id, o.order_date, MIN (o.order_date) over (partition by c.customer_id) as earliestorderdate
from customers c inner join orders o 
on c.customer_id = o.customer_id 

/*MAX
 * 5. Seleccione el id de producto, el nombre de producto, el precio unitario, el id de
categoría y el precio unitario máximo para cada categoría de la tabla Products
 */

select p.product_id, p.product_name , p.unit_price, c.category_id, MAX(p.unit_price) over (partition by p.category_id)
from products p inner join categories c 
on p.category_id = c.category_id 

/* Row_number
6. Obtener el ranking de los productos más vendidos VERRRRRR
*/ 

select RANK() over (partition by p.product_name order by od.quantity) as ranking,  p.product_name, sum(od.quantity) over (partition by p.product_name) as totalQty
from order_details od inner join products p
on od.product_id = p.product_id 
 
/* 7. Asignar numeros de fila para cada cliente, ordenados por customer_id*/  VERRRR

select row_number() over (partition by c.customer_id)as rownumber, c.customer_id, c.company_name, c.contact_name , c.contact_title, c.address 
from customers c 

/* 8. Obtener el ranking de los empleados más jóvenes () ranking, nombre y apellido del
empleado, fecha de nacimiento) */

select rank() over (order by e.birth_date desc),concat(e.last_name,' ',e.first_name)as employeename, e.birth_date 
from employees e 

/*  SUM
9. Obtener la suma de venta de cada cliente */

select SUM(od.unit_price*od.quantity) over (partition by c.customer_id)as sumorderamount, od.order_id, c.customer_id, o.employee_id, o.order_date, o.required_date
from (customers c inner join orders o  
on c.customer_id = o.customer_id ) inner join order_details od 
on od.order_id = o.order_id

/* 10.Obtener la suma total de ventas por categoría de producto */ VERRRR

select c.category_name, p.product_name, od.unit_price, od.quantity, SUM(od.unit_price*od.quantity) over (partition by c.category_id) as totalSales
from (order_details od inner join products p 
on od.product_id = p.product_id) inner join categories c 
on c.category_id = p.category_id

/* 11. Calcular la suma total de gastos de envío por país de destino, luego ordenarlo por país
y por orden de manera ascendente */

select o.ship_country, o.order_id, o.shipped_date, freight, SUM (freight) over (partition by ship_country order by ship_country asc) 
from orders o 

/* RANK
12.Ranking de ventas por cliente */

select distinct c.customer_id, c.company_name, SUM (od.unit_price*od.quantity) over (partition by c.customer_id) as Ventas, rank () over (partition by c.customer_id) as Ranking
from (customers c inner join orders o 
on c.customer_id = o.customer_id) inner join order_details od 
on od.order_id = o.order_id
order by ventas desc 

/* 13.Ranking de empleados por fecha de contratacion */

select e.employee_id , e.first_name, e.last_name, hire_date, rank() over (order by hire_date) as Ranking
from employees e 

/* 
14.Ranking de productos por precio unitario */


select product_id, product_name, unit_price, rank () over (order by unit_price desc)
from products p 

/* LAG
15.Mostrar por cada producto de una orden, la cantidad vendida y la cantidad
vendida del producto previo. */

select o.order_id , od.product_id , od.quantity, lag(od.quantity) over (order by o.order_id)
from orders o inner join order_details od 
on o.order_id = od.order_id 

/*16.Obtener un listado de ordenes mostrando el id de la orden, fecha de orden, id del cliente
y última fecha de orden*/

select o.order_id, o.order_date , o.customer_id, lag(order_date) over (partition by customer_id order by o.order_date)
from orders o 

/* 17.Obtener un listado de productos que contengan: id de producto, nombre del producto,
precio unitario, precio del producto anterior, diferencia entre el precio del producto y
precio del producto anterior.*/

select p.product_id, p.product_name, p.unit_price, lag(unit_price) over (order by p.product_id), (p.unit_price - (lag(unit_price) over (order by p.product_id))) as pricedifference
from products p 

/* LEAD
18.Obtener un listado que muestra el precio de un producto junto con el precio del producto
siguiente: */

select product_name , unit_price, lead (p.unit_price) over (order by p.product_id) as nextprice
from products p 

/* 19.Obtener un listado que muestra el total de ventas por categoría de producto junto con el
total de ventas de la categoría siguiente */  VERRRRRRRRRRR 




select distinct c.category_name, sum(od.unit_price*od.quantity) over (partition by p.category_id) as suma
from (products p inner join order_details od 
on p.product_id = od.product_id) inner join categories c 
on c.category_id = p.category_id

