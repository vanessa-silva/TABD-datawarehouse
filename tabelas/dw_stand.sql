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
-- Name: dw_stand; Type: TABLE; Schema: public; Owner: guest
--

CREATE TABLE dw_stand (
    stand_id integer NOT NULL,
    nome text NOT NULL
);


ALTER TABLE dw_stand OWNER TO guest;

--
-- Data for Name: dw_stand; Type: TABLE DATA; Schema: public; Owner: guest
--

COPY dw_stand (stand_id, nome) FROM stdin;
1	Agra
2	Alameda
3	Aldoar
4	Alfândega
5	Amial
6	Areosa
7	Av. Boavista
8	Azevedo
9	Batalha
10	Bolhão
11	Bom Pastor
12	Bom Sucesso
13	Brasília
14	Câmara
15	Campanhã
16	Campismo
17	Carcereira
18	Carregal
19	Carvalheiras
20	Carvalhido
21	Casa da Musica
22	Castelo do Queijo
23	Clérigos
24	Conde Ferreira
25	Cordoaria
26	Corujeira
27	D. João I
28	Dragão
29	Francos
30	Galiza
31	H. Magalhães Lemos
32	H. Militar
33	H. São João
34	Infante
35	Lago
36	Lordelo
37	Marechal
38	Marquês
39	Maternidade
40	Mercado da Foz
41	Nevogilde
42	Nove de Abril
43	Ouro
44	Palácio
45	Passeio Alegre
46	Pereira de Melo
47	Pereiró
48	Pinto de Azevedo
49	Pólo Universitário
50	Prelada
51	Ramada Alta
52	Républica
53	Ribeira
54	Rotunda
55	São Roque
56	Sá Carneiro
57	São Bento
58	Terço
59	Tenente Valadim
60	Trindade
61	Vinte e Quatro de Agosto
62	Viso
63	Santa Maria
\.


--
-- Name: dw_stand_pkey; Type: CONSTRAINT; Schema: public; Owner: guest
--

ALTER TABLE ONLY dw_stand
    ADD CONSTRAINT dw_stand_pkey PRIMARY KEY (stand_id);


--
-- PostgreSQL database dump complete
--

