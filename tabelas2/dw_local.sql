--
-- PostgreSQL database dump
--

-- Dumped from database version 9.5.4
-- Dumped by pg_dump version 9.5.4

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: dw_local; Type: TABLE; Schema: public; Owner: guest
--

CREATE TABLE dw_local (
    local_id integer NOT NULL,
    stand_id integer,
    freguesia text NOT NULL,
    concelho text NOT NULL
);


ALTER TABLE dw_local OWNER TO guest;

--
-- Data for Name: dw_local; Type: TABLE DATA; Schema: public; Owner: guest
--

COPY dw_local (local_id, stand_id, freguesia, concelho) FROM stdin;
1	1	PARANHOS	PORTO
2	2	CAMPANHA	PORTO
3	3	ALDOAR	PORTO
4	4	MIRAGAIA	PORTO
5	5	PARANHOS	PORTO
6	6	PARANHOS	PORTO
7	7	LORDELO DO OURO	PORTO
8	8	CAMPANHA	PORTO
9	9	S	PORTO
10	10	SANTO ILDEFONSO	PORTO
11	11	PARANHOS	PORTO
12	12	MASSARELOS	PORTO
13	13	CEDOFEITA	PORTO
14	14	SANTO ILDEFONSO	PORTO
15	15	CAMPANHA	PORTO
16	16	RAMALDE	PORTO
17	17	RAMALDE	PORTO
18	18	MIRAGAIA	PORTO
19	19	SANTO ILDEFONSO	PORTO
20	20	RAMALDE	PORTO
21	21	CEDOFEITA	PORTO
22	22	NEVOGILDE	PORTO
23	23	VITORIA	PORTO
24	24	PARANHOS	PORTO
25	25	VITORIA	PORTO
26	26	CAMPANHA	PORTO
27	27	SANTO ILDEFONSO	PORTO
28	28	CAMPANHA	PORTO
29	29	RAMALDE	PORTO
30	30	MASSARELOS	PORTO
31	31	ALDOAR	PORTO
32	32	CEDOFEITA	PORTO
33	33	PARANHOS	PORTO
34	34	S. NICOLAU	PORTO
35	35	RAMALDE	PORTO
36	36	LORDELO DO OURO	PORTO
37	37	LORDELO DO OURO	PORTO
38	38	BONFIM	PORTO
39	39	MASSARELOS	PORTO
40	40	FOZ DO DOURO	PORTO
41	41	ALDOAR	PORTO
42	42	PARANHOS	PORTO
43	43	LORDELO DO OURO	PORTO
44	44	MASSARELOS	PORTO
45	45	FOZ DO DOURO	PORTO
46	46	RAMALDE	PORTO
47	47	RAMALDE	PORTO
48	48	RAMALDE	PORTO
49	49	PARANHOS	PORTO
50	50	RAMALDE	PORTO
51	51	CEDOFEITA	PORTO
52	52	CEDOFEITA	PORTO
53	53	S. NICOLAU	PORTO
54	54	MASSARELOS	PORTO
55	55	CAMPANHA	PORTO
56	56	BONFIM	PORTO
57	57	S	PORTO
58	58	SANTO ILDEFONSO	PORTO
59	59	LORDELO DO OURO	PORTO
60	60	SANTO ILDEFONSO	PORTO
61	61	BONFIM	PORTO
62	62	RAMALDE	PORTO
63	63	SANTO ILDEFONSO	PORTO
\.


--
-- Name: dw_local_pkey; Type: CONSTRAINT; Schema: public; Owner: guest
--

ALTER TABLE ONLY dw_local
    ADD CONSTRAINT dw_local_pkey PRIMARY KEY (local_id);


--
-- Name: dw_local_stand_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: guest
--

ALTER TABLE ONLY dw_local
    ADD CONSTRAINT dw_local_stand_id_fkey FOREIGN KEY (stand_id) REFERENCES dw_stand(stand_id);


--
-- PostgreSQL database dump complete
--

