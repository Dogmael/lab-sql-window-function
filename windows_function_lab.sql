USE sakila;
/* 1. Find the running total of rental payments for each customer in the payment table, ordered by payment date. 
 By selecting the customer_id, payment_date, and amount columns from the payment table, and then applying the SUM function to the amount column within each customer_id partition, ordering by payment_date.
 */
SELECT customer_id,
    amount,
    payment_date,
    amount,
    sum(amount) OVER(
        PARTITION BY customer_id
        ORDER BY payment_date
    ) AS running_total
FROM payment;

/* 2.  Find the rank and dense rank of each payment amount within each payment date by selecting the payment_date and amount columns from the payment table, 
 and then applying the RANK and DENSE_RANK functions to the amount column within each payment_date partition, ordering by amount in descending order.
 */
SELECT payment_date,
    amount,
    RANK() OVER(
        PARTITION BY DATE(payment_date)
        ORDER BY amount DESC
    ) my_rank,
    -- Ne pas oublier DATE() !!!
    DENSE_RANK() OVER(
        PARTITION BY DATE(payment_date)
        ORDER BY amount DESC
    ) my_rank
FROM payment;

/* 3. Find the ranking of each film based on its rental rate, within its respective category. 
 */
SELECT category.name,
    film.title,
    film.rental_rate,
    RANK() OVER(
        PARTITION BY film_category.category_id
        ORDER BY rental_rate DESC
    ) rnk,
    DENSE_RANK() OVER(
        PARTITION BY film_category.category_id
        ORDER BY rental_rate DESC
    ) dens_rank
FROM film
    LEFT JOIN film_category ON film.film_id = film_category.film_id
    LEFT JOIN category ON category.category_id = film_category.category_id;

/* 4.(OPTIONAL) update the previous query from above to retrieve only the top 5 films within each category
 */
-- On ne peut pas faire de "where" statement sur des colonnes définies par "AS" (ici row_num) directement, on doit passé par des CTE (https://stackoverflow.com/questions/8370114/referring-to-a-column-alias-in-a-where-clause)
SELECT *
FROM (
        SELECT category.name,
            film.title,
            film.rental_rate,
            RANK() OVER(
                PARTITION BY film_category.category_id
                ORDER BY rental_rate DESC
            ) rnk,
            DENSE_RANK() OVER(
                PARTITION BY film_category.category_id
                ORDER BY rental_rate DESC
            ) dens_rank,
            ROW_NUMBER() OVER(
                PARTITION BY film_category.category_id
                ORDER BY rental_rate DESC
            ) AS row_num
        FROM film
            LEFT JOIN film_category ON film.film_id = film_category.film_id
            LEFT JOIN category ON category.category_id = film_category.category_id
    ) as innerTable
WHERE row_num <= 5;

/* 5. find the difference between the current and previous payment amount and the difference between the current and next payment amount, for each customer in the payment table
 Hint: select the payment_id, customer_id, amount, and payment_date columns from the payment table, and then applying the LAG and LEAD functions to the amount column, partitioning by customer_id and ordering by payment_date.
 
 payment_id customer_id amount payment_date diff_from_prev diff_from_next
 lead(expr, offset, default) − the value for the row offset rows after the current; offset and default are optional; default values: offset = 1, default = NULL
 lag(expr, offset, default) − the value for the row offset rows before the current; offset and default are optional; default values: offset = 1, default = NULL
 */
SELECT payment_id,
    customer_id,
    amount,
    payment_date,
    amount - LAG(amount) OVER(
        PARTITION BY customer_id
        ORDER BY payment_date
    ) AS diff_from_prev,
    amount - LEAD(amount) OVER(
        PARTITION BY customer_id
        ORDER BY payment_date
    ) AS diff_from_prev
FROM payment;