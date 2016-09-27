-- COSC 460 Fall 2016, Lab 4

-- These set the output format.  Please be sure to leave these settings as is.
.header OFF
.mode list

-- For each of the queries below, put your SQL in the place indicated by the comment.
-- Be sure to have all the requested columns in your answer, in the order they are
-- listed in the question - and be sure to sort things where the question requires
-- them to be sorted, and eliminate duplicates where the question requires that.
-- I will grade the assignment by running the queries on a test database and
-- eyeballing the SQL queries where necessary.  I won't grade on SQL style, but
-- I also won't give partial credit for any individual question - so you should be
-- confident that your query works. At the very least, your output should match
-- the example output.


-- Q1: Find all pizzas either Amy or Fay (or both) eat.
select " ";
select "Q1";
-- Put your SQL for Q1 below
select distinct pizza
from Eats
where name='Amy' or name='Fay';

-- Q2: Find all pizzas that both Amy and Fay eat.
select " ";
select "Q2";
-- Put your SQL for Q2 below
select distinct pizza
from Eats
where name='Amy'
intersect
select distinct pizza
from Eats
where name='Fay';

-- Q3: Find the price of the most expensive pizza served by each pizzeria.
select " ";
select "Q3";
-- Put your SQL for Q3 below
select distinct pizzeria, price
from Serves S
where not exists (select *
                  from Serves S2
                  where S.price<S2.price and S.pizzeria=S2.pizzeria);


-- Q4: Find the number of distinct kinds of pizza eaten by each gender.
select " ";
select "Q4";
-- Put your SQL for Q4 below
select gender,count(distinct pizzeria)
from Person P, Frequents F
where P.name= F.name
group by gender;
-- Q5: Find the names of pizzerias who serves at least 4 kinds of pizza.
select " ";
select "Q5";
select pizzeria
from (select pizzeria, count(*) as numPi
                      from Serves
                      group by pizzeria)
where numPi>=4;
-- Put your SQL for Q5 below

-- Q6: Find all pizzas that no one eats.
select " ";
select "Q6";
-- Put your SQL for Q6 below
select distinct pizza from Serves
except
select distinct pizza from Eats;

-- Q7: For each pizza, the number of individuals who eat that kind of pizza.  Your result should include pizzas that are served somewhere but eaten by no one -- for these pizzas, the number of individuals is zero.
select " ";
select "Q7";
-- Put your SQL for Q7 below
select pizza, count(*) as numP
from Eats
group by pizza
union
select pizza, 0 as numP
from (select distinct pizza from Serves
except
select distinct pizza from Eats);


-- Q8: Find the average number of pizzas made by a pizzeria.  Hint: first do a subquery that computes the number of pizzas made by each pizzeria.
select " ";
select "Q8";
-- Put your SQL for Q8 below
select avg(Nump)
from (select count(pizza) as Nump,pizzeria from Serves group by pizzeria);
-- Q9: Find the average number of pizzas eaten by men and women.
select " ";
select "Q9";
-- Put your SQL for Q9 below
select gender, avg(Nump)
from (select count(pizza) as Nump, gender from Person P,Eats E where P.name=E.name group by P.name)
group by gender;
-- Q10: Find all pizzerias that serve only pizzas eaten by people over 30.
select " ";
select "Q10";
-- Put your SQL for Q10 below
select pizzeria
from Serves
except
select pizzeria
from Serves S
where S.pizza not in
(select distinct pizza
  from Person P, Eats E
  where E.name=P.name and P.age >30);
