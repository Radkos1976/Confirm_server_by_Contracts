--
-- PostgreSQL database dump
--

-- Dumped from database version 15.4
-- Dumped by pg_dump version 15.4

-- Started on 2024-01-08 12:46:26

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 2 (class 3079 OID 17175)
-- Name: uuid-ossp; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;


--
-- TOC entry 3724 (class 0 OID 0)
-- Dependencies: 2
-- Name: EXTENSION "uuid-ossp"; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION "uuid-ossp" IS 'generate universally unique identifiers (UUIDs)';


--
-- TOC entry 333 (class 1255 OID 17186)
-- Name: Empty_notexist(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public."Empty_notexist"() RETURNS trigger
    LANGUAGE plpgsql
    AS $$BEGIN
IF TG_OP = 'UPDATE' THEN
	IF NEW.table_name='day_qty' AND NEW.in_progress=false AND NEW.updt_errors=false THEN
		DELETE FROM public.day_qty WHERE wrk_count(work_day)||'_'||typ||'_'||wrkc||'_'||next_wrkc in (select b.id from (select wrk_count(work_day)||'_'||typ||'_'||wrkc||'_'||next_wrkc id from day_qty) b left join day_qty_ifs a on a.id=b.id where a.id is null);
	END IF;
END IF;
return new;
END;
$$;


ALTER FUNCTION public."Empty_notexist"() OWNER TO postgres;

--
-- TOC entry 342 (class 1255 OID 17187)
-- Name: Hist_braki(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public."Hist_braki"() RETURNS trigger
    LANGUAGE plpgsql
    AS $$BEGIN  	       
IF TG_OP = 'UPDATE' then
insert into public.braki_hist values(
	new.ordid, new.l_ordid, new.indeks, new.opis, new.planner_buyer, new.mag, new.data_dost, new.date_reuired, new.wlk_dost, new.bilans, new.typ_zdarzenia, new.status_informacji, new.dop, new.dop_lin, new.data_dop, new.zlec, new.prod_date, new.max_posible_prod, new.max_prod_date, new.ord_supp_dmd, new.part_code, new.ord_state, new.prod_qty, new.qty_supply, new.qty_demand, new.koor, new.order_no, new.line_no, new.rel_no, new.part_no, new.descr, new.configuration, new.last_mail_conf, new.prom_date, new.prom_week, new.load_id, new.ship_date, new.state_conf, new.line_state, new.cust_ord_state, new.country, new.shipment_day, new.date_entered, new.sort_ord, new.zest, new.ord_assinged, new.id, new.cust_id,current_timestamp,TG_OP,new.umiejsc);
return NEW;
ELSEIF (TG_OP = 'INSERT') then
insert into public.braki_hist values(
	new.ordid, new.l_ordid, new.indeks, new.opis, new.planner_buyer, new.mag, new.data_dost, new.date_reuired, new.wlk_dost, new.bilans, new.typ_zdarzenia, new.status_informacji, new.dop, new.dop_lin, new.data_dop, new.zlec, new.prod_date, new.max_posible_prod, new.max_prod_date, new.ord_supp_dmd, new.part_code, new.ord_state, new.prod_qty, new.qty_supply, new.qty_demand, new.koor, new.order_no, new.line_no, new.rel_no, new.part_no, new.descr, new.configuration, new.last_mail_conf, new.prom_date, new.prom_week, new.load_id, new.ship_date, new.state_conf, new.line_state, new.cust_ord_state, new.country, new.shipment_day, new.date_entered, new.sort_ord, new.zest, new.ord_assinged, new.id, new.cust_id,current_timestamp,TG_OP,new.umiejsc);
return NEW;
ELSEIF (TG_OP = 'DELETE') THEN
insert into public.braki_hist values(
	old.ordid, old.l_ordid, old.indeks, old.opis, old.planner_buyer, old.mag, old.data_dost, old.date_reuired, old.wlk_dost, old.bilans, old.typ_zdarzenia, old.status_informacji, old.dop, old.dop_lin, old.data_dop, old.zlec, old.prod_date, old.max_posible_prod, old.max_prod_date, old.ord_supp_dmd, old.part_code, old.ord_state, old.prod_qty, old.qty_supply, old.qty_demand, old.koor, old.order_no, old.line_no, old.rel_no, old.part_no, old.descr, old.configuration, old.last_mail_conf, old.prom_date, old.prom_week, old.load_id, old.ship_date, old.state_conf, old.line_state, old.cust_ord_state, old.country, old.shipment_day, old.date_entered, old.sort_ord, old.zest, old.ord_assinged, old.id, old.cust_id,current_timestamp,TG_OP,old.umiejsc);
return OLD;
end if;
END;


$$;


ALTER FUNCTION public."Hist_braki"() OWNER TO postgres;

--
-- TOC entry 334 (class 1255 OID 17188)
-- Name: Hist_mail(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public."Hist_mail"() RETURNS trigger
    LANGUAGE plpgsql
    AS $$BEGIN  	       
IF TG_OP = 'UPDATE' then
insert into public.mail_hist values(
	new.ordid, new.dop, new.koor, new.order_no, new.line_no, new.rel_no, new.part_no, new.descr, new.country, new.prom_date, new.prom_week, new.load_id,new.ship_date, new.date_entered, new.cust_id, new.prod, new.prod_week, new.planner_buyer, new.indeks, new.opis, new.typ_zdarzenia, new.status_informacji, new.zest, new.info_handlo, new.logistyka, new.seria0, new.data0, new.cust_line_stat, new.ord_objver,current_timestamp,TG_OP);
return NEW;
ELSEIF (TG_OP = 'INSERT') then
insert into public.mail_hist values(
	new.ordid, new.dop, new.koor, new.order_no, new.line_no, new.rel_no, new.part_no, new.descr, new.country, new.prom_date, new.prom_week, new.load_id,new.ship_date, new.date_entered, new.cust_id, new.prod, new.prod_week, new.planner_buyer, new.indeks, new.opis, new.typ_zdarzenia, new.status_informacji, new.zest, new.info_handlo, new.logistyka, new.seria0, new.data0, new.cust_line_stat, new.ord_objver,current_timestamp,TG_OP);
return NEW;
ELSEIF (TG_OP = 'DELETE') THEN
insert into public.mail_hist values(
	old.ordid, old.dop, old.koor, old.order_no, old.line_no, old.rel_no, old.part_no, old.descr, old.country, old.prom_date, old.prom_week, old.load_id, old.ship_date, old.date_entered, old.cust_id, old.prod, old.prod_week, old.planner_buyer, old.indeks, old.opis, old.typ_zdarzenia, old.status_informacji, old.zest, old.info_handlo, old.logistyka, old.seria0, old.data0, old.cust_line_stat, old.ord_objver,current_timestamp,TG_OP);
return OLD;
end if;
END;
$$;


ALTER FUNCTION public."Hist_mail"() OWNER TO postgres;

--
-- TOC entry 289 (class 1255 OID 17189)
-- Name: addinfo_purch(character varying, date, character varying, double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.addinfo_purch(indeks character varying, data_dost date, info character varying, dost_ilosc double precision) RETURNS integer
    LANGUAGE plpgsql
    AS $_$DECLARE
wynik integer;
BEGIN
IF EXISTS (SELECT * from public.potw b where (b.indeks=$1 and b.data_dost=$2) or (b.indeks=$1 and b.rodzaj_potw='NIE ZAMAWIAM')) THEN
UPDATE public.potw b set info=$3 WHERE (b.indeks=$1 and b.data_dost=$2) or (b.indeks=$1 and b.rodzaj_potw='NIE ZAMAWIAM');
wynik=1;
ELSE
INSERT INTO public.potw (indeks, dost_ilosc, data_dost,info,date_created, id) values ($1,$4,$2,$3,current_timestamp,md5(random()::text || clock_timestamp()::text)::uuid);
wynik=0;
END IF;
DELETE FROM public.potw a WHERE a.rodzaj_potw='BRAK';
RETURN wynik;
END;
$_$;


ALTER FUNCTION public.addinfo_purch(indeks character varying, data_dost date, info character varying, dost_ilosc double precision) OWNER TO postgres;

--
-- TOC entry 290 (class 1255 OID 17959)
-- Name: calendar_id(character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.calendar_id(contract character varying) RETURNS character varying
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $_$
SELECT calendar_id FROM public.contract_calendar where contract = $1 limit 1
$_$;


ALTER FUNCTION public.calendar_id(contract character varying) OWNER TO postgres;

--
-- TOC entry 291 (class 1255 OID 17190)
-- Name: confirm_purch(character varying, double precision, date, character varying, date, character varying, double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.confirm_purch(indeks character varying, dost_ilosc double precision, data_dost date, rodzaj_potw character varying, termin_wazn date, koor character varying, sum_dost double precision) RETURNS integer
    LANGUAGE plpgsql
    AS $_$DECLARE
wynik integer;
BEGIN
IF EXISTS (SELECT * from public.potw b where (b.indeks=$1 and b.data_dost=$3) or (b.indeks=$1 and b.rodzaj_potw='NIE ZAMAWIAM')) THEN
UPDATE public.potw b SET indeks=$1, dost_ilosc=$2, data_dost=$3, sum_dost=$7, rodzaj_potw=$4, termin_wazn=$5, koor=$6, date_created=current_timestamp	WHERE (b.indeks=$1 and b.data_dost=$3) or (b.indeks=$1 and b.rodzaj_potw='NIE ZAMAWIAM');
wynik=1;
ELSE
INSERT INTO public.potw(indeks, dost_ilosc, data_dost, sum_dost, rodzaj_potw, termin_wazn, koor, date_created, id) VALUES ($1,$2,$3,$7,$4,$5,$6,current_timestamp,md5(random()::text || clock_timestamp()::text)::uuid);
wynik=0;
END IF;
DELETE FROM public.potw a WHERE a.rodzaj_potw='BRAK';                 
RETURN wynik;
END;
$_$;


ALTER FUNCTION public.confirm_purch(indeks character varying, dost_ilosc double precision, data_dost date, rodzaj_potw character varying, termin_wazn date, koor character varying, sum_dost double precision) OWNER TO postgres;

--
-- TOC entry 292 (class 1255 OID 17191)
-- Name: cust_lin_stat(uuid); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.cust_lin_stat(cust_id uuid) RETURNS character varying
    LANGUAGE sql PARALLEL SAFE
    AS $_$
select line_state from cust_ord where id=$1
$_$;


ALTER FUNCTION public.cust_lin_stat(cust_id uuid) OWNER TO postgres;

--
-- TOC entry 293 (class 1255 OID 17192)
-- Name: date_fromnow(integer); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.date_fromnow("DAYS" integer) RETURNS date
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $_$select wrk_day(COALESCE(now_counter(),now_near_counter())+$1);$_$;


ALTER FUNCTION public.date_fromnow("DAYS" integer) OWNER TO postgres;

--
-- TOC entry 3732 (class 0 OID 0)
-- Dependencies: 293
-- Name: FUNCTION date_fromnow("DAYS" integer); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION public.date_fromnow("DAYS" integer) IS 'Zwraca date od dzis o liczbe dni';


--
-- TOC entry 303 (class 1255 OID 17966)
-- Name: date_fromnow(integer, character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.date_fromnow("DAYS" integer, contract character varying) RETURNS date
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $_$
select wrk_day(COALESCE(now_counter($2),now_near_counter($2))+$1);
$_$;


ALTER FUNCTION public.date_fromnow("DAYS" integer, contract character varying) OWNER TO postgres;

--
-- TOC entry 294 (class 1255 OID 17193)
-- Name: date_ser0(uuid); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.date_ser0(cust_id uuid) RETURNS date
    LANGUAGE sql PARALLEL SAFE
    AS $_$
select data0 from cust_ord where id=$1
$_$;


ALTER FUNCTION public.date_ser0(cust_id uuid) OWNER TO postgres;

--
-- TOC entry 295 (class 1255 OID 17194)
-- Name: date_shift_days(date, integer); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.date_shift_days("FROM_DAYS" date, "SHIFT_DAYS" integer) RETURNS date
    LANGUAGE sql PARALLEL SAFE
    AS $_$select coalesce(wrk_day(COALESCE(wrk_count($1),wrk_near_count($1))+$2),wrk_day(wrk_near_count($1)));
$_$;


ALTER FUNCTION public.date_shift_days("FROM_DAYS" date, "SHIFT_DAYS" integer) OWNER TO postgres;

--
-- TOC entry 3736 (class 0 OID 0)
-- Dependencies: 295
-- Name: FUNCTION date_shift_days("FROM_DAYS" date, "SHIFT_DAYS" integer); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION public.date_shift_days("FROM_DAYS" date, "SHIFT_DAYS" integer) IS 'Przesunięcie o zadaną liczbę dni w kalendarzu IFS';


--
-- TOC entry 304 (class 1255 OID 17965)
-- Name: date_shift_days(date, integer, character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.date_shift_days("FROM_DAYS" date, "SHIFT_DAYS" integer, contract character varying) RETURNS date
    LANGUAGE sql PARALLEL SAFE
    AS $_$
select coalesce(wrk_day(COALESCE(wrk_count($1, $3),wrk_near_count($1,$3))+$2),wrk_day(wrk_near_count($1,$3)));
$_$;


ALTER FUNCTION public.date_shift_days("FROM_DAYS" date, "SHIFT_DAYS" integer, contract character varying) OWNER TO postgres;

--
-- TOC entry 335 (class 1255 OID 17195)
-- Name: day_qty_notnull(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.day_qty_notnull() RETURNS trigger
    LANGUAGE plpgsql COST 30
    AS $$BEGIN
IF TG_OP='INSERT' or TG_OP='UPDATE' THEN
	if new.brak is null THEN
		new.brak=0;
	END IF;
	return NEW;	
END IF;
END;
$$;


ALTER FUNCTION public.day_qty_notnull() OWNER TO postgres;

--
-- TOC entry 336 (class 1255 OID 17196)
-- Name: div_zero_day_qty(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.div_zero_day_qty() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
DELETE FROM public.day_qty WHERE qty_all=0;
return NEW;
END;
$$;


ALTER FUNCTION public.div_zero_day_qty() OWNER TO postgres;

--
-- TOC entry 296 (class 1255 OID 17197)
-- Name: dmd_type(integer); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.dmd_type("TYPE_code" integer DEFAULT 0) RETURNS character varying
    LANGUAGE sql STRICT PARALLEL SAFE
    AS $_$
Select type_dmd from public.type_dmd where id=$1
$_$;


ALTER FUNCTION public.dmd_type("TYPE_code" integer) OWNER TO postgres;

--
-- TOC entry 3741 (class 0 OID 0)
-- Dependencies: 296
-- Name: FUNCTION dmd_type("TYPE_code" integer); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION public.dmd_type("TYPE_code" integer) IS 'Typ potrzeby';


--
-- TOC entry 343 (class 1255 OID 31060)
-- Name: get_contract_from_dop(integer); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_contract_from_dop("DOP_ID" integer) RETURNS character varying
    LANGUAGE sql
    AS $_$Select contract from cust_ord where dop_id=$1 limit 1$_$;


ALTER FUNCTION public.get_contract_from_dop("DOP_ID" integer) OWNER TO postgres;

--
-- TOC entry 297 (class 1255 OID 17198)
-- Name: get_date_dop(uuid); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_date_dop(cust_id uuid) RETURNS date
    LANGUAGE sql PARALLEL SAFE
    AS $_$
select data_dop from cust_ord where id=$1
$_$;


ALTER FUNCTION public.get_date_dop(cust_id uuid) OWNER TO postgres;

--
-- TOC entry 298 (class 1255 OID 17199)
-- Name: get_dopstat(uuid); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_dopstat(cust_id uuid) RETURNS character varying
    LANGUAGE sql PARALLEL SAFE
    AS $_$
select dop_state from cust_ord where id=$1
$_$;


ALTER FUNCTION public.get_dopstat(cust_id uuid) OWNER TO postgres;

--
-- TOC entry 316 (class 1255 OID 17200)
-- Name: get_inventory(character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_inventory(part_no character varying) RETURNS double precision
    LANGUAGE sql COST 80
    AS $_$
select mag from public.mag where indeks=$1
$_$;


ALTER FUNCTION public.get_inventory(part_no character varying) OWNER TO postgres;

--
-- TOC entry 269 (class 1255 OID 31058)
-- Name: get_inventory(character varying, character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_inventory(part_no character varying, contract character varying) RETURNS double precision
    LANGUAGE sql COST 80
    AS $_$
select mag from public.mag where indeks=$1 and contract=$2
$_$;


ALTER FUNCTION public.get_inventory(part_no character varying, contract character varying) OWNER TO postgres;

--
-- TOC entry 299 (class 1255 OID 17201)
-- Name: get_koor(character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_koor(part_no character varying) RETURNS character varying
    LANGUAGE sql PARALLEL SAFE
    AS $_$
select planner_buyer from mag where indeks=$1
$_$;


ALTER FUNCTION public.get_koor(part_no character varying) OWNER TO postgres;

--
-- TOC entry 300 (class 1255 OID 17202)
-- Name: get_refer(character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_refer(addr1 character varying) RETURNS character varying
    LANGUAGE plpgsql PARALLEL SAFE
    AS $_$declare
pos integer;
pos1 integer;
BEGIN
    pos:= instr($1,'<<');
    if pos>0 then 
    	pos1:= instr($1,'>>');
        if pos1>0 then
        	if pos<pos1 then
            	return substring ($1,pos+2,pos1-pos-2);
            else 
            	return null;
               end if;
    	 else 
            return null;
         end if;
    else 
    return null; 
    end if;    
END;


$_$;


ALTER FUNCTION public.get_refer(addr1 character varying) OWNER TO postgres;

--
-- TOC entry 345 (class 1255 OID 17203)
-- Name: getltfrompart(character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.getltfrompart(descr character varying) RETURNS double precision
    LANGUAGE plpgsql STABLE
    AS $_$
DECLARE
lt integer;
onlyone integer;
tmpint double precision;
tmp_lt double precision;
tmp varchar;

BEGIN
tmp_lt:=200;
onlyone:=0;
lt=strpos($1,';');
If lt=0 then 
	lt:=strpos($1,':');
	onlyone:=1;
end if;

IF LT>0 then
	IF onlyone=1 then
		return (select czas_dostawy from public.mag where indeks=substring($1,LT+1));
	else
		tmp=substring($1,lt+1);
		if strpos(tmp,';')=0 then
			return (select czas_dostawy from public.mag where indeks=tmp);
		else
			while strpos(tmp,';')>0 loop
				tmpint=(select czas_dostawy from public.mag where indeks=substring(tmp,1,strpos(tmp,';')));
				if tmpint<tmp_lt then 
					tmp_lt=tmpint;
				end if;
				tmp=substring(tmp,strpos(tmp,';')+1);
			end loop;
			tmpint=(select czas_dostawy from public.mag where indeks=tmp);
			if tmpint<tmp_lt then 
				tmp_lt=tmpint;
			end if;
			return tmp_lt;
		end if;
	end if; 
else
	return (select czas_dostawy from public.mag where indeks=$1);
end if;

END;
$_$;


ALTER FUNCTION public.getltfrompart(descr character varying) OWNER TO postgres;

--
-- TOC entry 344 (class 1255 OID 31059)
-- Name: getltfrompart(character varying, character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.getltfrompart(descr character varying, contract character varying) RETURNS double precision
    LANGUAGE plpgsql STABLE
    AS $_$
DECLARE
lt integer;
onlyone integer;
tmpint double precision;
tmp_lt double precision;
tmp varchar;

BEGIN
tmp_lt:=200;
onlyone:=0;
lt=strpos($1,';');
If lt=0 then 
	lt:=strpos($1,':');
	onlyone:=1;
end if;

IF LT>0 then
	IF onlyone=1 then
		return (select czas_dostawy from public.mag where indeks=substring($1,LT+1) and public.mag.contract=$2);
	else
		tmp=substring($1,lt+1);
		if strpos(tmp,';')=0 then
			return (select czas_dostawy from public.mag where indeks=tmp);
		else
			while strpos(tmp,';')>0 loop
				tmpint=(select czas_dostawy from public.mag where indeks=substring(tmp,1,strpos(tmp,';')) and public.mag.contract=$2);
				if tmpint<tmp_lt then 
					tmp_lt=tmpint;
				end if;
				tmp=substring(tmp,strpos(tmp,';')+1);
			end loop;
			tmpint=(select czas_dostawy from public.mag where indeks=tmp and public.mag.contract=$2);
			if tmpint<tmp_lt then 
				tmp_lt=tmpint;
			end if;
			return tmp_lt;
		end if;
	end if; 
else
	return (select czas_dostawy from public.mag where indeks=$1 and public.mag.contract=$2);
end if;

END;
$_$;


ALTER FUNCTION public.getltfrompart(descr character varying, contract character varying) OWNER TO postgres;

--
-- TOC entry 337 (class 1255 OID 17204)
-- Name: hist_send_mail(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.hist_send_mail() RETURNS trigger
    LANGUAGE plpgsql
    AS $$BEGIN  	       
IF TG_OP = 'UPDATE' then
	if new.last_mail is not null then
	insert into public.send_mail_hist values(
		new.mail, new.typ, new.typ_zdarzenia, new.status_informacji, new.info, new.corr, new.cust_ord, new.c_lin, new.c_rel, new.catalog_desc, new.c_ry, new.load_id, new.ship_date, new.prom_week, new.prod_week, new.prod_date, new.part_buyer, new.shortage_part, new.short_nam, new.dop, new.created, new.last_mail, current_timestamp );
	end if;
return NEW;
end if;
end;
$$;


ALTER FUNCTION public.hist_send_mail() OWNER TO postgres;

--
-- TOC entry 305 (class 1255 OID 17205)
-- Name: how_many(integer); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.how_many(dop integer) RETURNS integer
    LANGUAGE sql
    AS $_$select how_many from cust_ord where dop_id=$1$_$;


ALTER FUNCTION public.how_many(dop integer) OWNER TO postgres;

--
-- TOC entry 306 (class 1255 OID 17206)
-- Name: instr(character varying, character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.instr(character varying, character varying) RETURNS integer
    LANGUAGE plpgsql
    AS $_$DECLARE
    pos integer;
BEGIN
    pos:= instr($1, $2, 1);
    RETURN pos;
END;
$_$;


ALTER FUNCTION public.instr(character varying, character varying) OWNER TO postgres;

--
-- TOC entry 307 (class 1255 OID 17207)
-- Name: instr(character varying, character varying, integer); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.instr(character varying, character varying, integer) RETURNS integer
    LANGUAGE plpgsql
    AS $_$DECLARE
    string ALIAS FOR $1;
    string_to_search ALIAS FOR $2;
    beg_index ALIAS FOR $3;
    pos integer DEFAULT 0;
    temp_str varchar;
    beg integer;
    length integer;
    ss_length integer;
BEGIN
    IF beg_index > 0 THEN
        temp_str := substring(string FROM beg_index);
        pos := position(string_to_search IN temp_str);

        IF pos = 0 THEN
            RETURN 0;
        ELSE
            RETURN pos + beg_index - 1;
        END IF;
    ELSE
        ss_length := char_length(string_to_search);
        length := char_length(string);
        beg := length + beg_index - ss_length + 2;

        WHILE beg > 0 LOOP
            temp_str := substring(string FROM beg FOR ss_length);
            pos := position(string_to_search IN temp_str);

            IF pos > 0 THEN
                RETURN beg;
            END IF;

            beg := beg - 1;
        END LOOP;

        RETURN 0;
    END IF;
END;
$_$;


ALTER FUNCTION public.instr(character varying, character varying, integer) OWNER TO postgres;

--
-- TOC entry 308 (class 1255 OID 17208)
-- Name: instr(character varying, character varying, integer, integer); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.instr(character varying, character varying, integer, integer) RETURNS integer
    LANGUAGE plpgsql
    AS $_$
DECLARE
    string ALIAS FOR $1;
    string_to_search ALIAS FOR $2;
    beg_index ALIAS FOR $3;
    occur_index ALIAS FOR $4;
    pos integer NOT NULL DEFAULT 0;
    occur_number integer NOT NULL DEFAULT 0;
    temp_str varchar;
    beg integer;
    i integer;
    length integer;
    ss_length integer;
BEGIN
    IF beg_index > 0 THEN
        beg := beg_index;
        temp_str := substring(string FROM beg_index);

        FOR i IN 1..occur_index LOOP
            pos := position(string_to_search IN temp_str);

            IF i = 1 THEN
                beg := beg + pos - 1;
            ELSE
                beg := beg + pos;
            END IF;

            temp_str := substring(string FROM beg + 1);
        END LOOP;

        IF pos = 0 THEN
            RETURN 0;
        ELSE
            RETURN beg;
        END IF;
    ELSE
        ss_length := char_length(string_to_search);
        length := char_length(string);
        beg := length + beg_index - ss_length + 2;

        WHILE beg > 0 LOOP
            temp_str := substring(string FROM beg FOR ss_length);
            pos := position(string_to_search IN temp_str);

            IF pos > 0 THEN
                occur_number := occur_number + 1;

                IF occur_number = occur_index THEN
                    RETURN beg;
                END IF;
            END IF;

            beg := beg - 1;
        END LOOP;

        RETURN 0;
    END IF;
END;
$_$;


ALTER FUNCTION public.instr(character varying, character varying, integer, integer) OWNER TO postgres;

--
-- TOC entry 309 (class 1255 OID 17209)
-- Name: is_alter(character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.is_alter(status_informacji character varying) RETURNS boolean
    LANGUAGE plpgsql PARALLEL SAFE
    AS $_$
declare
pos integer;
BEGIN
    pos:= instr($1,'Użyj FR');
    if pos>0 then 
    return true;
    else 
    return false; 
    end if;    
END;
$_$;


ALTER FUNCTION public.is_alter(status_informacji character varying) OWNER TO postgres;

--
-- TOC entry 310 (class 1255 OID 17210)
-- Name: is_confirm(character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.is_confirm(status_informacji character varying) RETURNS boolean
    LANGUAGE plpgsql STABLE PARALLEL SAFE
    AS $_$
declare
pos integer;
BEGIN
    pos:= instr($1,'POTWIERDZONE');
    if pos>0 then 
    return true;
    else 
    return false; 
    end if;    
END;
$_$;


ALTER FUNCTION public.is_confirm(status_informacji character varying) OWNER TO postgres;

--
-- TOC entry 311 (class 1255 OID 17211)
-- Name: is_dontpurch(character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.is_dontpurch(status_informacji character varying) RETURNS boolean
    LANGUAGE plpgsql PARALLEL SAFE
    AS $_$
declare
pos integer;
BEGIN
    pos:= instr($1,'NIE ZAMAWIAM');
    if pos>0 then 
    return true;
    else 
    return false; 
    end if;    
END;
$_$;


ALTER FUNCTION public.is_dontpurch(status_informacji character varying) OWNER TO postgres;

--
-- TOC entry 312 (class 1255 OID 17212)
-- Name: is_for_mail(character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.is_for_mail(status_informacji character varying) RETURNS boolean
    LANGUAGE plpgsql
    AS $_$declare
pos integer;
BEGIN
    pos:= instr($1,'POTWIERDZONE')+instr($1,'NIE ZAMAWIAM')+instr($1,'POPRAWIĆ')+instr($1,'Użyj FR');
    if pos>0 then 
    return true;
    else 
    return false; 
    end if;    
END;
$_$;


ALTER FUNCTION public.is_for_mail(status_informacji character varying) OWNER TO postgres;

--
-- TOC entry 313 (class 1255 OID 17213)
-- Name: is_interncl(uuid); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.is_interncl(cust_id uuid) RETURNS boolean
    LANGUAGE sql PARALLEL SAFE
    AS $_$
select case when cust_no='556' then true else false end from cust_ord where id=$1
$_$;


ALTER FUNCTION public.is_interncl(cust_id uuid) OWNER TO postgres;

--
-- TOC entry 314 (class 1255 OID 17214)
-- Name: is_refer(character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.is_refer(addr1 character varying) RETURNS boolean
    LANGUAGE plpgsql STABLE PARALLEL SAFE
    AS $_$
declare
pos integer;
pos1 integer;
BEGIN
    pos:= instr($1,'<<');
    if pos>0 then 
    	pos1:= instr($1,'>>');
        if pos1>0 then
        	if pos<pos1 then
            	return true;
            else 
            	return false;
               end if;
    	 else 
            return false;
         end if;
    else 
    return false; 
    end if;    
END;


$_$;


ALTER FUNCTION public.is_refer(addr1 character varying) OWNER TO postgres;

--
-- TOC entry 315 (class 1255 OID 17215)
-- Name: is_seria0(uuid); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.is_seria0(cust_id uuid) RETURNS boolean
    LANGUAGE sql PARALLEL SAFE
    AS $_$
select seria0 from cust_ord where id=$1
$_$;


ALTER FUNCTION public.is_seria0(cust_id uuid) OWNER TO postgres;

--
-- TOC entry 271 (class 1255 OID 17216)
-- Name: late_ord_exist(uuid); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.late_ord_exist(cust_id uuid) RETURNS boolean
    LANGUAGE plpgsql
    AS $_$DECLARE
wynik boolean;
BEGIN
IF EXISTS (SELECT b.cust_id from public.late_ord b where b.cust_id=$1) THEN
wynik=true;
ELSE
wynik=false;
END IF;
RETURN wynik;
END;
$_$;


ALTER FUNCTION public.late_ord_exist(cust_id uuid) OWNER TO postgres;

--
-- TOC entry 272 (class 1255 OID 17217)
-- Name: mag_type(character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.mag_type(indeks character varying) RETURNS character varying
    LANGUAGE sql PARALLEL SAFE
    AS $_$
select rodzaj from mag where indeks=$1
$_$;


ALTER FUNCTION public.mag_type(indeks character varying) OWNER TO postgres;

--
-- TOC entry 338 (class 1255 OID 17218)
-- Name: mod_ord_confirm_date(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.mod_ord_confirm_date() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
chk bool=false;
message text=' '; 
BEGIN
IF TG_OP='UPDATE' THEN
	chk = false;
   if new.prom_date<>old.prom_date THEN
   		message = message || 'Confirm_date;';
		chk = true;
	END IF;
	if new.ship_date<>old.ship_date THEN
		message = message || 'Shipment_date;';
		chk = true;
	END IF;
	if new.configuration<>old.configuration THEN
		message = message || 'Configuration;';
		chk = true;
	END IF;	
	if chk = true then
		insert into public.cust_ord_history values(
		old.koor, old.order_no, old.line_no, old.rel_no, old.line_item_no, old.customer_po_line_no, old.dimmension, old.last_mail_conf, old.state_conf, old.line_state, old.cust_order_state, old.country, old.cust_no, old.zip_code, old.addr1, old.prom_date, old.prom_week, old.load_id, old.ship_date, old.part_no, old.descr, old.configuration, old.buy_qty_due, old.desired_qty, old.qty_invoiced, old.qty_shipped, old.qty_assigned, old.dop_connection_db, old.dop_id, old.dop_state, old.data_dop, old.dop_qty, old.dop_made, old.date_entered, old.chksum, old.custid, old.id, old.zest, old.seria0, old.data0, old.objversion,message,current_timestamp,old.how_many);
	END IF;
	return NEW;
END IF;
END;
$$;


ALTER FUNCTION public.mod_ord_confirm_date() OWNER TO postgres;

--
-- TOC entry 3764 (class 0 OID 0)
-- Dependencies: 338
-- Name: FUNCTION mod_ord_confirm_date(); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION public.mod_ord_confirm_date() IS 'Wyzwalacz do śledzenia danych które nie są archiwizowane w poprawny sposób w IFS';


--
-- TOC entry 339 (class 1255 OID 17219)
-- Name: note(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.note() RETURNS trigger
    LANGUAGE plpgsql
    AS $$ declare
 txt text;
    BEGIN    	       
        IF TG_OP = 'UPDATE'  THEN
        txt := 'Data aktualizacji:'|| (select server_updt()) || ' Status serwera(DANE):'|| (select server_state());
			 perform pg_notify ('zakupy', txt::text) ; 
        END IF;
        -- ok, zwracamy nowy rekord
return new;
END;
$$;


ALTER FUNCTION public.note() OWNER TO postgres;

--
-- TOC entry 286 (class 1255 OID 17220)
-- Name: now_counter(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.now_counter() RETURNS integer
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $$
select counter from public.work_cal where calendar_id = calendar_id('ST') and work_day=current_date;
$$;


ALTER FUNCTION public.now_counter() OWNER TO postgres;

--
-- TOC entry 3767 (class 0 OID 0)
-- Dependencies: 286
-- Name: FUNCTION now_counter(); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION public.now_counter() IS 'Numer dzisiejszego roboczego dnia w kalendarzu';


--
-- TOC entry 273 (class 1255 OID 17964)
-- Name: now_counter(character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.now_counter(contract character varying) RETURNS integer
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $_$
select counter from public.work_cal where calendar_id = calendar_id($1) and work_day=current_date;
$_$;


ALTER FUNCTION public.now_counter(contract character varying) OWNER TO postgres;

--
-- TOC entry 274 (class 1255 OID 17221)
-- Name: now_near_counter(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.now_near_counter() RETURNS integer
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $$
select counter from public.work_cal where calendar_id = calendar_id('ST') and work_day>=current_date order by counter,work_day limit 1;
$$;


ALTER FUNCTION public.now_near_counter() OWNER TO postgres;

--
-- TOC entry 3770 (class 0 OID 0)
-- Dependencies: 274
-- Name: FUNCTION now_near_counter(); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION public.now_near_counter() IS 'Numer najblizszego biezacego dnia w kalendarzu';


--
-- TOC entry 301 (class 1255 OID 17963)
-- Name: now_near_counter(character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.now_near_counter(contract character varying) RETURNS integer
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $_$
select counter from public.work_cal where calendar_id = calendar_id($1) and work_day>=current_date order by counter,work_day limit 1;
$_$;


ALTER FUNCTION public.now_near_counter(contract character varying) OWNER TO postgres;

--
-- TOC entry 287 (class 1255 OID 17222)
-- Name: ord_objver(uuid); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.ord_objver(cust_id uuid) RETURNS timestamp without time zone
    LANGUAGE sql PARALLEL SAFE
    AS $_$
select objversion from cust_ord where id=$1
$_$;


ALTER FUNCTION public.ord_objver(cust_id uuid) OWNER TO postgres;

--
-- TOC entry 340 (class 1255 OID 17223)
-- Name: refr_views_tmp(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.refr_views_tmp() RETURNS trigger
    LANGUAGE plpgsql
    AS $$BEGIN
IF TG_OP = 'INSERT' THEN
REFRESH MATERIALIZED VIEW braki_poreal;
REFRESH MATERIALIZED VIEW braki_gniazd;
return NEW;
END IF;
END;
$$;


ALTER FUNCTION public.refr_views_tmp() OWNER TO postgres;

--
-- TOC entry 288 (class 1255 OID 17224)
-- Name: server_state(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.server_state() RETURNS character varying
    LANGUAGE plpgsql
    AS $$declare 
wynik varchar;
begin
wynik=(select coalesce(string_agg(table_name, ', '),'WAIT') from datatbles where table_name!='server_progress' and in_progress=true);
return wynik;
end;
$$;


ALTER FUNCTION public.server_state() OWNER TO postgres;

--
-- TOC entry 302 (class 1255 OID 17225)
-- Name: server_updt(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.server_updt() RETURNS timestamp without time zone
    LANGUAGE sql PARALLEL SAFE
    AS $$
select last_modify from datatbles where table_name='server_progress'
$$;


ALTER FUNCTION public.server_updt() OWNER TO postgres;

--
-- TOC entry 317 (class 1255 OID 17226)
-- Name: shipment_day(character varying, character varying, character varying, character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.shipment_day(country_code character varying, cust_no character varying, zip_code character varying, addr1 character varying) RETURNS integer
    LANGUAGE plpgsql
    AS $_$
DECLARE
wynik integer;
BEGIN
CASE UPPER($1)
	WHEN 'CH','PL','SE' THEN
    	CASE $2 
			WHEN '4774','268','1232','1736','2956','2413','1539','91','2747','322','295','3296','214','785','273','280','288','290','1669','314','4112','3422','987','1884','334','354','301','270','4524','166','1410','1199','1409','4037','316','284','276','274','302','275','4594','4453','321','1048','1449','2455','1380','1352','235','2385','269','265','311','3969','289','266','3187','346','556'  then
        		wynik=5;
        	ELSE 
    			wynik=3;
        END CASE;
    WHEN 'IE','GB' THEN
    	wynik=2;
    WHEN 'NO' THEN
    	CASE $2
        	WHEN '2248','2282' THEN
            	wynik=2;
            ELSE
            	IF $3='4353' THEN
                	wynik=2;
                ELSE
                	wynik=4;
                END IF;
        END CASE;
     WHEN 'DK' THEN
     	IF UPPER(substring($4 from 1 for 2))='LG' THEN
        	wynik=2;
        ELSE
			IF upper(substring($4 from 1  for 5))='BOLIA' THEN
			wynik=6;
			ELSE
        	wynik=5;
			END IF;
        END IF;
     ELSE	 	
     	wynik=5;
END CASE;
RETURN wynik;
END;
$_$;


ALTER FUNCTION public.shipment_day(country_code character varying, cust_no character varying, zip_code character varying, addr1 character varying) OWNER TO postgres;

--
-- TOC entry 3777 (class 0 OID 0)
-- Dependencies: 317
-- Name: FUNCTION shipment_day(country_code character varying, cust_no character varying, zip_code character varying, addr1 character varying); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION public.shipment_day(country_code character varying, cust_no character varying, zip_code character varying, addr1 character varying) IS 'dzien wysyłki';


--
-- TOC entry 341 (class 1255 OID 17227)
-- Name: try_update_orders(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.try_update_orders() RETURNS trigger
    LANGUAGE plpgsql
    AS $$BEGIN
IF (TG_OP = 'INSERT' AND new.err=true) then
	update public.demands set chksum=chksum+1 from (select indeks as part_no,date_reuired as work_day from public.braki where dop=new.dop) a where public.demands.part_no=a.part_no and public.demands.work_day=a.work_day;
END IF;
return NEW;
END;$$;


ALTER FUNCTION public.try_update_orders() OWNER TO postgres;

--
-- TOC entry 3779 (class 0 OID 0)
-- Dependencies: 341
-- Name: FUNCTION try_update_orders(); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION public.try_update_orders() IS 'Zmienia sumę kontrolną w tabeli DEMANDS - wymusza ponowne pobranie rekordów dotyczących potrzeb na konkretnym dniu. ';


--
-- TOC entry 318 (class 1255 OID 17228)
-- Name: updt_dta_potw(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.updt_dta_potw() RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
wynik integer;
busy integer;
BEGIN
LOOP
	select cast(count(table_name) as integer) into busy from public.datatbles where (table_name='data' or table_name='potw') and in_progress=true;	
	IF busy=0 THEN
		EXIT;
	END IF;
	SELECT pg_sleep(0.5);
END LOOP;
SET TRANSACTION ISOLATION LEVEL Read committed;
UPDATE public.data b SET status_informacji=a.potw,informacja=a.info from (select a.id,case when a.typ_zdarzenia in ('Dzisiejsza dostawa','Opóźniona dostawa','Nieaktualne Dostawy') then null else coalesce(b.rodzaj_potw,'BRAK') end potw,a.status_informacji,b.info from public.data a left join potw b on b.indeks=a.indeks and (b.data_dost=a.data_dost or b.rodzaj_potw='NIE ZAMAWIAM') where coalesce (a.status_informacji,'N')!=coalesce(case when a.typ_zdarzenia in ('Dzisiejsza dostawa','Opóźniona dostawa','Nieaktualne Dostawy') then null else coalesce(b.rodzaj_potw,'BRAK') end,'N') or coalesce(a.informacja,'n')!=coalesce(b.info,'n'))  a  where b.id=a.id;
wynik=0;
RETURN wynik;
END;
$$;


ALTER FUNCTION public.updt_dta_potw() OWNER TO postgres;

--
-- TOC entry 329 (class 1255 OID 17229)
-- Name: weektodat(character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.weektodat(week character varying) RETURNS date
    LANGUAGE sql PARALLEL RESTRICTED
    AS $_$
select to_date($1, 'iyyyiw');
$_$;


ALTER FUNCTION public.weektodat(week character varying) OWNER TO postgres;

--
-- TOC entry 330 (class 1255 OID 17230)
-- Name: wrk_count(date); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.wrk_count("WORK_DAY" date) RETURNS integer
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $_$
Select coalesce(counter,wrk_near_count($1)) from public.work_cal where calendar_id = calendar_id('ST') and work_day>=$1 order by work_day limit 1;
$_$;


ALTER FUNCTION public.wrk_count("WORK_DAY" date) OWNER TO postgres;

--
-- TOC entry 3793 (class 0 OID 0)
-- Dependencies: 330
-- Name: FUNCTION wrk_count("WORK_DAY" date); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION public.wrk_count("WORK_DAY" date) IS 'Pobierz numer dnia';


--
-- TOC entry 331 (class 1255 OID 17961)
-- Name: wrk_count(date, character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.wrk_count("WORK_DAY" date, contract character varying) RETURNS integer
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $_$
Select coalesce(counter,wrk_near_count($1, calendar_id(contract))) from public.work_cal where calendar_id = calendar_id($2) and work_day>=$1 order by work_day limit 1;
$_$;


ALTER FUNCTION public.wrk_count("WORK_DAY" date, contract character varying) OWNER TO postgres;

--
-- TOC entry 332 (class 1255 OID 17231)
-- Name: wrk_day(integer); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.wrk_day("COUNTER" integer) RETURNS date
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $_$
Select work_day from public.work_cal where calendar_id = calendar_id('ST') and counter=$1 ;
$_$;


ALTER FUNCTION public.wrk_day("COUNTER" integer) OWNER TO postgres;

--
-- TOC entry 3796 (class 0 OID 0)
-- Dependencies: 332
-- Name: FUNCTION wrk_day("COUNTER" integer); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION public.wrk_day("COUNTER" integer) IS 'Zwroc date wg numeru dnia';


--
-- TOC entry 268 (class 1255 OID 17962)
-- Name: wrk_day(integer, character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.wrk_day("COUNTER" integer, contract character varying) RETURNS date
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $_$
Select work_day from public.work_cal where calendar_id = calendar_id($2) and counter=$1 ;
$_$;


ALTER FUNCTION public.wrk_day("COUNTER" integer, contract character varying) OWNER TO postgres;

--
-- TOC entry 270 (class 1255 OID 17232)
-- Name: wrk_near_count(date); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.wrk_near_count("WORK_DAY" date) RETURNS integer
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $_$
Select counter from public.work_cal where calendar_id = calendar_id('ST') and work_day>=$1 order by counter,work_day limit 1;
$_$;


ALTER FUNCTION public.wrk_near_count("WORK_DAY" date) OWNER TO postgres;

--
-- TOC entry 3799 (class 0 OID 0)
-- Dependencies: 270
-- Name: FUNCTION wrk_near_count("WORK_DAY" date); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION public.wrk_near_count("WORK_DAY" date) IS 'Zwraca najbliższy dzień w kalendarzu';


--
-- TOC entry 267 (class 1255 OID 17960)
-- Name: wrk_near_count(date, character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.wrk_near_count("WORK_DAY" date, "CONTRACT" character varying) RETURNS integer
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $_$
Select counter from public.work_cal where calendar_id = calendar_id($2) and work_day>=$1 order by counter,work_day limit 1;
$_$;


ALTER FUNCTION public.wrk_near_count("WORK_DAY" date, "CONTRACT" character varying) OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 215 (class 1259 OID 17233)
-- Name: CRP; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public."CRP" (
    id bigint NOT NULL,
    work_day date,
    department_no character varying(10),
    work_center_no character varying(10),
    capacity double precision,
    planned double precision,
    relased double precision,
    "DOP" double precision
);


ALTER TABLE public."CRP" OWNER TO postgres;

--
-- TOC entry 3802 (class 0 OID 0)
-- Dependencies: 215
-- Name: TABLE "CRP"; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public."CRP" IS 'Obciążenie zasobów produkcyjnych ';


--
-- TOC entry 216 (class 1259 OID 17236)
-- Name: cust_ord; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.cust_ord (
    koor character varying(15),
    order_no character varying(15),
    line_no character varying(5),
    rel_no character varying(5),
    line_item_no integer,
    customer_po_line_no character varying(5),
    dimmension double precision,
    last_mail_conf date,
    state_conf character varying(15),
    line_state character varying(30),
    cust_order_state character varying(30),
    country character varying(3),
    cust_no character varying(30),
    zip_code character varying(35),
    addr1 character varying(250),
    prom_date date,
    prom_week character varying(6),
    load_id integer,
    ship_date date,
    part_no character varying(25),
    descr character varying(150),
    configuration character varying(15),
    buy_qty_due double precision,
    desired_qty double precision,
    qty_invoiced double precision,
    qty_shipped double precision,
    qty_assigned double precision,
    dop_connection_db character varying(8),
    dop_id integer,
    dop_state character varying(15),
    data_dop date,
    dop_qty double precision,
    dop_made double precision,
    date_entered timestamp without time zone,
    chksum integer,
    custid integer,
    id uuid NOT NULL,
    zest character varying(50),
    seria0 boolean,
    data0 date,
    objversion timestamp without time zone,
    how_many integer,
    contract character varying DEFAULT 'ST'::character varying
);


ALTER TABLE public.cust_ord OWNER TO postgres;

--
-- TOC entry 217 (class 1259 OID 17241)
-- Name: send_mail_hist; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.send_mail_hist (
    mail character varying(250) NOT NULL,
    typ character varying(150),
    typ_zdarzenia character varying(500),
    status_informacji character varying(500),
    info character varying(500),
    corr character varying(15),
    cust_ord character varying(15),
    c_lin character varying(5),
    c_rel character varying(5),
    catalog_desc character varying(150),
    c_ry character varying(10),
    load_id character varying(10),
    ship_date date,
    prom_week integer,
    prod_week integer,
    prod_date date,
    part_buyer character varying(250),
    shortage_part character varying(500),
    short_nam character varying(1000),
    dop integer,
    created timestamp without time zone,
    last_mail timestamp without time zone,
    date_add timestamp without time zone
);


ALTER TABLE public.send_mail_hist OWNER TO postgres;

--
-- TOC entry 218 (class 1259 OID 17246)
-- Name: Liczba zmain terminów ; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public."Liczba zmain terminów " AS
 SELECT b.changes,
    c.prom_week,
    c.line_state,
    c.dop_made,
    a.corr,
    a.cust_ord,
    a.c_lin,
    a.c_rel,
    a.catalog_desc,
    string_agg((a.part_buyer)::text, ';'::text ORDER BY (a.part_buyer)::text) AS buyer,
    string_agg((a.shortage_part)::text, ';'::text ORDER BY (a.shortage_part)::text) AS shortage_part,
    string_agg((a.short_nam)::text, ';'::text ORDER BY a.shortage_part) AS short_nam,
    a.dop,
    a.created
   FROM ( SELECT a_1.corr,
            a_1.cust_ord,
            a_1.c_lin,
            a_1.c_rel,
            a_1.catalog_desc,
            a_1.part_buyer,
            a_1.shortage_part,
            a_1.short_nam,
            a_1.dop,
            a_1.created
           FROM public.send_mail_hist a_1
          WHERE (((a_1.status_informacji)::text ~~ '%POTWIERDZONE%'::text) AND ((a_1.typ)::text = 'MAIL'::text))
          GROUP BY a_1.corr, a_1.cust_ord, a_1.c_lin, a_1.c_rel, a_1.catalog_desc, a_1.part_buyer, a_1.shortage_part, a_1.short_nam, a_1.dop, a_1.created) a,
    (( SELECT a_1.dop,
            count(a_1.prod_week) AS changes
           FROM ( SELECT send_mail_hist.dop,
                    send_mail_hist.prod_week
                   FROM public.send_mail_hist
                  WHERE (((send_mail_hist.status_informacji)::text ~~ '%POTWIERDZONE%'::text) AND ((send_mail_hist.typ)::text = 'MAIL'::text))
                  GROUP BY send_mail_hist.dop, send_mail_hist.prod_week) a_1
          GROUP BY a_1.dop) b
     LEFT JOIN public.cust_ord c ON ((c.dop_id = b.dop)))
  WHERE (b.dop = a.dop)
  GROUP BY c.prom_week, c.line_state, c.dop_made, a.corr, a.cust_ord, a.c_lin, a.c_rel, a.catalog_desc, a.dop, a.created, b.changes
  ORDER BY b.changes DESC, c.dop_made, c.prom_week, a.cust_ord DESC;


ALTER TABLE public."Liczba zmain terminów " OWNER TO postgres;

--
-- TOC entry 219 (class 1259 OID 17251)
-- Name: braki; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.braki (
    ordid text,
    l_ordid bigint,
    indeks character varying(25),
    opis character varying(150),
    planner_buyer character varying(10),
    mag double precision,
    data_dost date,
    date_reuired date,
    wlk_dost double precision,
    bilans double precision,
    typ_zdarzenia character varying(35),
    status_informacji character varying(35),
    dop integer,
    dop_lin integer,
    data_dop date,
    zlec character varying(25),
    prod_date date,
    max_posible_prod date,
    max_prod_date date,
    ord_supp_dmd character varying(25),
    part_code character varying(20),
    ord_state character varying(20),
    prod_qty double precision,
    qty_supply double precision,
    qty_demand double precision,
    koor character varying(15),
    order_no character varying(15),
    line_no character varying(5),
    rel_no character varying(5),
    part_no character varying(25),
    descr character varying(150),
    configuration character varying(15),
    last_mail_conf date,
    prom_date date,
    prom_week character varying(6),
    load_id bigint,
    ship_date date,
    state_conf character varying(15),
    line_state character varying(30),
    cust_ord_state character varying(30),
    country character varying(3),
    shipment_day integer,
    date_entered timestamp without time zone,
    sort_ord timestamp without time zone,
    zest character varying(50),
    ord_assinged double precision,
    id uuid NOT NULL,
    cust_id uuid,
    umiejsc character varying,
    data_gwarancji date
);


ALTER TABLE public.braki OWNER TO postgres;

--
-- TOC entry 3807 (class 0 OID 0)
-- Dependencies: 219
-- Name: TABLE braki; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.braki IS 'Macierz zlecen z brakami';


--
-- TOC entry 220 (class 1259 OID 17256)
-- Name: cust_ord_history; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.cust_ord_history (
    koor character varying(15),
    order_no character varying(15),
    line_no character varying(5),
    rel_no character varying(5),
    line_item_no integer,
    customer_po_line_no character varying(5),
    dimmension double precision,
    last_mail_conf date,
    state_conf character varying(15),
    line_state character varying(30),
    cust_order_state character varying(30),
    country character varying(3),
    cust_no character varying(30),
    zip_code character varying(35),
    addr1 character varying(250),
    prom_date date,
    prom_week character varying(6),
    load_id integer,
    ship_date date,
    part_no character varying(25),
    descr character varying(150),
    configuration character varying(15),
    buy_qty_due double precision,
    desired_qty double precision,
    qty_invoiced double precision,
    qty_shipped double precision,
    qty_assigned double precision,
    dop_connection_db character varying(8),
    dop_id integer,
    dop_state character varying(15),
    data_dop date,
    dop_qty double precision,
    dop_made double precision,
    date_entered timestamp without time zone,
    chksum integer,
    custid integer,
    id uuid NOT NULL,
    zest character varying(50),
    seria0 boolean,
    data0 date,
    objversion timestamp without time zone,
    operation character varying(50),
    date_add timestamp with time zone,
    how_many integer,
    contract character varying(20)
);


ALTER TABLE public.cust_ord_history OWNER TO postgres;

--
-- TOC entry 221 (class 1259 OID 17261)
-- Name: mail_hist; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.mail_hist (
    ordid text,
    dop integer,
    koor character varying(15),
    order_no character varying(15),
    line_no character varying(5),
    rel_no character varying(5),
    part_no character varying(25),
    descr character varying(150),
    country character varying(3),
    prom_date date,
    prom_week integer,
    load_id bigint,
    ship_date date,
    date_entered timestamp without time zone,
    cust_id uuid NOT NULL,
    prod date,
    prod_week integer,
    planner_buyer character varying(500),
    indeks character varying(500),
    opis character varying(1000),
    typ_zdarzenia character varying(1000),
    status_informacji character varying(500),
    zest character varying(50),
    info_handlo boolean,
    logistyka boolean,
    seria0 boolean,
    data0 date,
    cust_line_stat character varying(50),
    ord_objver timestamp without time zone,
    date_addd timestamp without time zone,
    "Db_type" character varying(7)
);


ALTER TABLE public.mail_hist OWNER TO postgres;

--
-- TOC entry 222 (class 1259 OID 17266)
-- Name: Mieszanie z Konfiguracjami; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public."Mieszanie z Konfiguracjami" AS
 SELECT ('D'::text || to_char(b.dop_id, '9999999999'::text)) AS ordid,
    a.dop_id AS dop,
    b.koor,
    b.order_no,
    b.line_no,
    b.rel_no,
    b.part_no,
    b.descr,
    b.country,
    b.prom_date,
    (b.prom_week)::integer AS prom_week,
    b.load_id,
    b.ship_date,
    a.date_add AS date_entered,
    b.id,
    a.data_dop AS prod,
    (
        CASE
            WHEN (date_part('isodow'::text, a.data_dop) < (public.shipment_day(b.country, b.cust_no, b.zip_code, b.addr1))::double precision) THEN to_char((a.data_dop)::timestamp with time zone, 'IYYYIW'::text)
            ELSE to_char((a.data_dop + '7 days'::interval), 'IYYYIW'::text)
        END)::integer AS prod_week,
    'RADKOS'::text AS planner_buyer,
    'KONFIGURACJA'::text AS indeks,
    'Ustawienie pierwotnej daty'::text AS opis,
    'Wewn. procedura'::text AS typ_zdarzenia,
    'WYKONANIE'::text AS status_informacji,
    b.zest
   FROM public.cust_ord b,
    (((( SELECT cust_ord_history.koor,
            cust_ord_history.order_no,
            cust_ord_history.line_no,
            cust_ord_history.rel_no,
            cust_ord_history.line_item_no,
            cust_ord_history.customer_po_line_no,
            cust_ord_history.dimmension,
            cust_ord_history.last_mail_conf,
            cust_ord_history.state_conf,
            cust_ord_history.line_state,
            cust_ord_history.cust_order_state,
            cust_ord_history.country,
            cust_ord_history.cust_no,
            cust_ord_history.zip_code,
            cust_ord_history.addr1,
            cust_ord_history.prom_date,
            cust_ord_history.prom_week,
            cust_ord_history.load_id,
            cust_ord_history.ship_date,
            cust_ord_history.part_no,
            cust_ord_history.descr,
            cust_ord_history.configuration,
            cust_ord_history.buy_qty_due,
            cust_ord_history.desired_qty,
            cust_ord_history.qty_invoiced,
            cust_ord_history.qty_shipped,
            cust_ord_history.qty_assigned,
            cust_ord_history.dop_connection_db,
            cust_ord_history.dop_id,
            cust_ord_history.dop_state,
            cust_ord_history.data_dop,
            cust_ord_history.dop_qty,
            cust_ord_history.dop_made,
            cust_ord_history.date_entered,
            cust_ord_history.chksum,
            cust_ord_history.custid,
            cust_ord_history.id,
            cust_ord_history.zest,
            cust_ord_history.seria0,
            cust_ord_history.data0,
            cust_ord_history.objversion,
            cust_ord_history.operation,
            cust_ord_history.date_add
           FROM public.cust_ord_history
          WHERE ((cust_ord_history.date_add > (CURRENT_TIMESTAMP - '3 days'::interval)) AND ((cust_ord_history.operation)::text ~~ '%Configuration;%'::text) AND ((cust_ord_history.dop_state)::text <> 'Unreleased'::text))) a
     LEFT JOIN ( SELECT b_1.dop_id,
            b_1.data_dop,
            a_1.date_add
           FROM ( SELECT cust_ord_history.dop_id,
                    max(cust_ord_history.date_add) AS date_add
                   FROM public.cust_ord_history
                  WHERE (cust_ord_history.date_add > (CURRENT_TIMESTAMP - '3 days'::interval))
                  GROUP BY cust_ord_history.dop_id) a_1,
            public.cust_ord_history b_1
          WHERE ((b_1.dop_id = a_1.dop_id) AND (b_1.date_add = a_1.date_add))) c ON (((c.dop_id = a.dop_id) AND (round(((date_part('epoch'::text, c.date_add) / (10)::double precision))::numeric, 0) < round(((date_part('epoch'::text, a.date_add) / (10)::double precision))::numeric, 0)))))
     LEFT JOIN ( SELECT mail_hist.cust_id,
            mail_hist.dop AS dop_id,
            mail_hist.date_entered
           FROM public.mail_hist
          WHERE ((mail_hist.date_addd > (CURRENT_TIMESTAMP - '3 days'::interval)) AND ((mail_hist.indeks)::text = 'KONFIGURACJA'::text) AND ((mail_hist."Db_type")::text = 'DELETE'::text))) d ON (((a.id = d.cust_id) AND (a.date_add = d.date_entered))))
     LEFT JOIN ( SELECT braki.cust_id
           FROM public.braki) g ON ((g.cust_id = a.id)))
  WHERE ((a.id = b.id) AND (g.cust_id IS NULL) AND ((a.configuration)::text <> '*'::text) AND (a.data_dop IS NOT NULL) AND (d.dop_id IS NULL) AND (b.prom_date = a.prom_date) AND (b.dop_made = (0)::double precision) AND (COALESCE(c.data_dop, b.data_dop) <> a.data_dop))
  ORDER BY a.date_add DESC;


ALTER TABLE public."Mieszanie z Konfiguracjami" OWNER TO postgres;

--
-- TOC entry 223 (class 1259 OID 17271)
-- Name: Past_day; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public."Past_day" (
    "typ_Raportu" character varying(30),
    wydz character varying(20),
    order_no character varying(50),
    descr character varying(300),
    qty_all double precision,
    qty double precision,
    revised_due_date timestamp without time zone,
    "zgłoszone" timestamp without time zone,
    all_h double precision,
    rest_h double precision,
    on_time double precision,
    in_time double precision,
    too_late double precision,
    made_to_late double precision,
    objversion uuid NOT NULL,
    refresh_dat timestamp without time zone
);


ALTER TABLE public."Past_day" OWNER TO postgres;

--
-- TOC entry 3812 (class 0 OID 0)
-- Dependencies: 223
-- Name: TABLE "Past_day"; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public."Past_day" IS 'Tabela przechowująca wyniki Archiwalne z Terminowości';


--
-- TOC entry 224 (class 1259 OID 17274)
-- Name: active_locks; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.active_locks AS
 SELECT t.schemaname,
    t.relname,
    l.locktype,
    l.page,
    l.virtualtransaction,
    l.pid,
    l.mode,
    l.granted
   FROM (pg_locks l
     JOIN pg_stat_all_tables t ON ((l.relation = t.relid)))
  WHERE ((t.schemaname <> 'pg_toast'::name) AND (t.schemaname <> 'pg_catalog'::name))
  ORDER BY t.schemaname, t.relname;


ALTER TABLE public.active_locks OWNER TO postgres;

--
-- TOC entry 225 (class 1259 OID 17279)
-- Name: demands; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.demands (
    part_no character varying(15) NOT NULL,
    work_day date NOT NULL,
    expected_leadtime smallint NOT NULL,
    purch_qty double precision NOT NULL,
    qty_demand double precision NOT NULL,
    type_dmd smallint,
    balance double precision NOT NULL,
    bal_stock double precision NOT NULL,
    koor character varying(8),
    type character varying(10),
    dat_shortage timestamp without time zone,
    id uuid NOT NULL,
    chk_sum double precision,
    objversion timestamp without time zone NOT NULL,
    chksum integer,
    indb boolean,
    contract character varying(15)
);


ALTER TABLE public.demands OWNER TO postgres;

--
-- TOC entry 226 (class 1259 OID 17282)
-- Name: aktual_hist; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.aktual_hist WITH (security_barrier='false') AS
 SELECT a.koor,
    a.work_day,
    a.type,
    a.sumal,
    a.brak,
    a.brak_mag
   FROM ( SELECT a_1.koor,
            ('now'::text)::date AS work_day,
            a_1.type,
            sum(a_1.qty_demand) AS sumal,
            sum(
                CASE
                    WHEN (a_1.brak < (0)::double precision) THEN a_1.brak
                    ELSE (0)::double precision
                END) AS brak,
            sum(
                CASE
                    WHEN (a_1.brak_mag < (0)::double precision) THEN a_1.brak_mag
                    ELSE (0)::double precision
                END) AS brak_mag
           FROM ( SELECT a_2.koor,
                    a_2.part_no,
                    ('now'::text)::date AS work_day,
                    a_2.type,
                    sum(a_2.qty_demand) AS qty_demand,
                    min(a_2.balance) AS brak,
                    min(a_2.bal_stock) AS brak_mag
                   FROM public.demands a_2
                  WHERE ((a_2.work_day <= ('now'::text)::date) AND ((a_2.koor)::text <> '*'::text) AND ((a_2.koor)::text <> 'LUCPRZ'::text))
                  GROUP BY a_2.koor, a_2.part_no, a_2.type) a_1
          WHERE ((a_1.work_day <= ('now'::text)::date) AND ((a_1.koor)::text <> '*'::text) AND ((a_1.koor)::text <> 'LUCPRZ'::text))
          GROUP BY a_1.koor, a_1.type
        UNION ALL
         SELECT a_1.koor,
            a_1.work_day,
            a_1.type,
            sum(a_1.qty_demand) AS sumal,
            sum(
                CASE
                    WHEN (a_1.balance < (0)::double precision) THEN
                    CASE
                        WHEN (a_1.balance < (a_1.qty_demand * ('-1'::integer)::double precision)) THEN (a_1.qty_demand * ('-1'::integer)::double precision)
                        ELSE a_1.balance
                    END
                    ELSE (0)::double precision
                END) AS brak,
            sum(
                CASE
                    WHEN (a_1.bal_stock < (0)::double precision) THEN
                    CASE
                        WHEN (a_1.bal_stock < (a_1.qty_demand * ('-1'::integer)::double precision)) THEN (a_1.qty_demand * ('-1'::integer)::double precision)
                        ELSE a_1.bal_stock
                    END
                    ELSE (0)::double precision
                END) AS brak_mag
           FROM public.demands a_1
          WHERE ((a_1.work_day > ('now'::text)::date) AND ((a_1.koor)::text <> '*'::text) AND ((a_1.koor)::text <> 'LUCPRZ'::text))
          GROUP BY a_1.koor, a_1.work_day, a_1.type) a
  ORDER BY a.koor, a.work_day, a.type;


ALTER TABLE public.aktual_hist OWNER TO postgres;

--
-- TOC entry 227 (class 1259 OID 17287)
-- Name: mag; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.mag (
    indeks character varying(25) NOT NULL,
    opis character varying(150),
    kolekcja character varying(40),
    mag double precision,
    planner_buyer character varying(10),
    rodzaj character varying(15),
    czas_dostawy double precision NOT NULL,
    weight_net double precision,
    volume_net double precision,
    inventory_value double precision,
    note_id integer NOT NULL,
    contract character varying NOT NULL,
    part_product_family character varying
);


ALTER TABLE public.mag OWNER TO postgres;

--
-- TOC entry 261 (class 1259 OID 27336)
-- Name: bilans_val; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.bilans_val AS
 SELECT a.planner_buyer,
    a.indeks AS part_no,
    a.contract,
    a.opis,
    a.mag AS il_magazyn,
    a.purch_all AS il_zam,
    a.dmd_all AS potrzeby,
    ((a.mag + a.purch_all) - a.dmd_all) AS bilans,
    a.va AS cena_jednostkowa,
    (((a.mag + a.purch_all) - a.dmd_all) * a.va) AS "wartość"
   FROM ( SELECT b.planner_buyer,
            b.indeks,
            b.contract,
            b.inventory_value AS va,
            b.opis,
            b.mag,
            COALESCE(sum(a_1.qty_demand), (0)::double precision) AS dmd_all,
            COALESCE(sum(a_1.purch_qty), (0)::double precision) AS purch_all
           FROM (public.mag b
             LEFT JOIN public.demands a_1 ON (((a_1.part_no)::text = (b.indeks)::text)))
          GROUP BY b.planner_buyer, b.indeks, b.contract, b.inventory_value, b.opis, b.mag) a
  WHERE (((a.mag + a.purch_all) + a.dmd_all) > (0)::double precision)
  ORDER BY a.planner_buyer, a.indeks
  WITH NO DATA;


ALTER TABLE public.bilans_val OWNER TO postgres;

--
-- TOC entry 228 (class 1259 OID 17295)
-- Name: day_qty; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.day_qty (
    work_day date,
    typ character varying(3),
    wrkc character varying(10),
    next_wrkc character varying(10),
    qty_all double precision,
    brak double precision,
    contract character varying
);


ALTER TABLE public.day_qty OWNER TO postgres;

--
-- TOC entry 265 (class 1259 OID 30315)
-- Name: braki_gniazd; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.braki_gniazd AS
 SELECT a.work_day,
    a.contract,
    a.typ,
        CASE
            WHEN ("substring"((a.wrkc)::text, 1, 1) = '4'::text) THEN
            CASE
                WHEN ("substring"((a.wrkc)::text, 1, 2) = ANY (ARRAY['40'::text, '46'::text])) THEN a.wrkc
                ELSE '400TAP'::character varying
            END
            ELSE a.wrkc
        END AS wrkc,
    sum(a.qty_all) AS qty_all,
    sum(COALESCE(a.brak, (0)::double precision)) AS brak
   FROM ( SELECT day_qty.work_day,
            day_qty.typ,
            day_qty.contract,
            day_qty.wrkc,
            day_qty.qty_all,
            day_qty.brak
           FROM public.day_qty
        UNION ALL
         SELECT day_qty.work_day,
            day_qty.typ,
            day_qty.contract,
            day_qty.next_wrkc AS wrkc,
            day_qty.qty_all,
            day_qty.brak
           FROM public.day_qty
          WHERE ((day_qty.next_wrkc)::text <> (day_qty.wrkc)::text)) a
  GROUP BY a.work_day, a.contract, a.typ,
        CASE
            WHEN ("substring"((a.wrkc)::text, 1, 1) = '4'::text) THEN
            CASE
                WHEN ("substring"((a.wrkc)::text, 1, 2) = ANY (ARRAY['40'::text, '46'::text])) THEN a.wrkc
                ELSE '400TAP'::character varying
            END
            ELSE a.wrkc
        END
  ORDER BY a.work_day,
        CASE
            WHEN ("substring"((a.wrkc)::text, 1, 1) = '4'::text) THEN
            CASE
                WHEN ("substring"((a.wrkc)::text, 1, 2) = ANY (ARRAY['40'::text, '46'::text])) THEN a.wrkc
                ELSE '400TAP'::character varying
            END
            ELSE a.wrkc
        END, a.typ
  WITH NO DATA;


ALTER TABLE public.braki_gniazd OWNER TO postgres;

--
-- TOC entry 229 (class 1259 OID 17305)
-- Name: braki_hist; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.braki_hist (
    ordid text,
    l_ordid bigint,
    indeks character varying(25),
    opis character varying(150),
    planner_buyer character varying(10),
    mag double precision,
    data_dost date,
    date_reuired date,
    wlk_dost double precision,
    bilans double precision,
    typ_zdarzenia character varying(35),
    status_informacji character varying(35),
    dop integer,
    dop_lin integer,
    data_dop date,
    zlec character varying(25),
    prod_date date,
    max_posible_prod date,
    max_prod_date date,
    ord_supp_dmd character varying(25),
    part_code character varying(20),
    ord_state character varying(20),
    prod_qty double precision,
    qty_supply double precision,
    qty_demand double precision,
    koor character varying(15),
    order_no character varying(15),
    line_no character varying(5),
    rel_no character varying(5),
    part_no character varying(25),
    descr character varying(150),
    configuration character varying(15),
    last_mail_conf date,
    prom_date date,
    prom_week character varying(6),
    load_id bigint,
    ship_date date,
    state_conf character varying(15),
    line_state character varying(30),
    cust_ord_state character varying(30),
    country character varying(3),
    shipment_day integer,
    date_entered timestamp without time zone,
    sort_ord timestamp without time zone,
    zest character varying(50),
    ord_assinged double precision,
    id uuid NOT NULL,
    cust_id uuid,
    objversion timestamp without time zone,
    dbtype character varying(25),
    umiejsc character varying
);


ALTER TABLE public.braki_hist OWNER TO postgres;

--
-- TOC entry 230 (class 1259 OID 17310)
-- Name: ord_lack; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ord_lack (
    dop integer NOT NULL,
    dop_lin integer,
    data_dop date,
    day_shift integer,
    order_no character varying(25),
    line_no character varying(5),
    rel_no character varying(5),
    int_ord integer,
    contract character varying(10),
    order_supp_dmd character varying(25),
    wrkc character varying(15),
    next_wrkc character varying(15),
    part_no character varying(15),
    descr character varying(150),
    part_code character varying(20),
    date_required date,
    ord_state character varying(20),
    ord_date date,
    prod_qty double precision,
    qty_supply double precision,
    qty_demand double precision,
    dat_creat date,
    chksum integer,
    id uuid NOT NULL
);


ALTER TABLE public.ord_lack OWNER TO postgres;

--
-- TOC entry 263 (class 1259 OID 27984)
-- Name: braki_poreal; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.braki_poreal AS
 SELECT a.part_no,
    a.contract,
    a.descr,
    b.mag,
    b.planner_buyer,
        CASE
            WHEN (a.ord_date < ('now'::text)::date) THEN ('now'::text)::date
            ELSE a.ord_date
        END AS date_required,
        CASE
            WHEN (a.dop = 0) THEN 'MRP'::text
            ELSE 'DOP'::text
        END AS typ,
    sum(a.prod_qty) AS "zagrożenie_prod",
        CASE
            WHEN ((a.wrkc)::text = ' '::text) THEN ' - '::character varying
            ELSE a.wrkc
        END AS wrkc,
        CASE
            WHEN ((a.next_wrkc)::text = ' '::text) THEN ' - '::character varying
            ELSE a.next_wrkc
        END AS next_wrkc
   FROM public.ord_lack a,
    public.mag b
  WHERE (((a.order_supp_dmd)::text <> 'Zam. zakupu'::text) AND ((b.indeks)::text = (a.part_no)::text) AND ((b.contract)::text = (a.contract)::text))
  GROUP BY a.part_no, a.contract, a.descr, b.mag, b.planner_buyer,
        CASE
            WHEN (a.ord_date < ('now'::text)::date) THEN ('now'::text)::date
            ELSE a.ord_date
        END,
        CASE
            WHEN (a.dop = 0) THEN 'MRP'::text
            ELSE 'DOP'::text
        END,
        CASE
            WHEN ((a.wrkc)::text = ' '::text) THEN ' - '::character varying
            ELSE a.wrkc
        END,
        CASE
            WHEN ((a.next_wrkc)::text = ' '::text) THEN ' - '::character varying
            ELSE a.next_wrkc
        END
  WITH NO DATA;


ALTER TABLE public.braki_poreal OWNER TO postgres;

--
-- TOC entry 231 (class 1259 OID 17320)
-- Name: mod_date; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.mod_date (
    order_no character varying(15),
    line_no character varying(5),
    rel_no character varying(5),
    prod date,
    typ_zdarzenia character varying(1000),
    status_informacji character varying(500),
    dop integer,
    err boolean,
    indeks character varying(500),
    opis character varying(1000),
    data_dop date,
    date_add timestamp without time zone
);


ALTER TABLE public.mod_date OWNER TO postgres;

--
-- TOC entry 232 (class 1259 OID 17325)
-- Name: work_cal; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.work_cal (
    calendar_id character varying(15),
    counter integer,
    work_day date,
    day_type character varying(15),
    working_time double precision,
    working_periods integer,
    objversion character varying(14)
);


ALTER TABLE public.work_cal OWNER TO postgres;

--
-- TOC entry 266 (class 1259 OID 31061)
-- Name: braki_poza_lt; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.braki_poza_lt AS
 SELECT a.order_no,
    a.line_no,
    a.rel_no,
    a.prod,
    a.typ_zdarzenia,
    a.status_informacji,
    a.dop,
    a.err,
    a.indeks,
    a.opis,
    a.data_dop,
    a.date_add
   FROM (public.work_cal b
     LEFT JOIN ( SELECT a_1.order_no,
            a_1.line_no,
            a_1.rel_no,
            a_1.prod,
            a_1.typ_zdarzenia,
            a_1.status_informacji,
            a_1.dop,
            a_1.err,
            a_1.indeks,
            a_1.opis,
            a_1.data_dop,
            a_1.date_add
           FROM public.mod_date a_1
          WHERE (((a_1.data_dop <= date(a_1.date_add)) OR (a_1.data_dop < public.date_shift_days(date(a_1.date_add), (public.getltfrompart(a_1.indeks, public.get_contract_from_dop(a_1.dop)))::integer))) AND (a_1.err = false))) a ON ((date(a.date_add) = b.work_day)))
  WHERE (a.date_add IS NOT NULL)
  WITH NO DATA;


ALTER TABLE public.braki_poza_lt OWNER TO postgres;

--
-- TOC entry 233 (class 1259 OID 17335)
-- Name: braki_tmp; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.braki_tmp (
    ordid text,
    l_ordid bigint,
    indeks character varying(25),
    opis character varying(150),
    planner_buyer character varying(10),
    mag double precision,
    data_dost date,
    date_reuired date,
    wlk_dost double precision,
    bilans double precision,
    typ_zdarzenia character varying(35),
    status_informacji character varying(35),
    dop integer,
    dop_lin integer,
    data_dop date,
    zlec character varying(25),
    prod_date date,
    max_posible_prod date,
    max_prod_date date,
    ord_supp_dmd character varying(25),
    part_code character varying(20),
    ord_state character varying(20),
    prod_qty double precision,
    qty_supply double precision,
    qty_demand double precision,
    koor character varying(15),
    order_no character varying(15),
    line_no character varying(5),
    rel_no character varying(5),
    part_no character varying(25),
    descr character varying(150),
    configuration character varying(15),
    last_mail_conf date,
    prom_date date,
    prom_week character varying(6),
    load_id bigint,
    ship_date date,
    state_conf character varying(15),
    line_state character varying(30),
    cust_ord_state character varying(30),
    country character varying(3),
    shipment_day integer,
    date_entered timestamp without time zone,
    sort_ord timestamp without time zone,
    zest character varying(50),
    ord_assinged double precision,
    id uuid NOT NULL,
    cust_id uuid
);


ALTER TABLE public.braki_tmp OWNER TO postgres;

--
-- TOC entry 3823 (class 0 OID 0)
-- Dependencies: 233
-- Name: TABLE braki_tmp; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.braki_tmp IS 'Macierz zlecen z brakami';


--
-- TOC entry 234 (class 1259 OID 17340)
-- Name: conf_mail_null; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.conf_mail_null (
    order_no character varying(15),
    cust_no character varying(30),
    reference character varying(100),
    mail character varying(500),
    country character varying(30),
    date_add date
);


ALTER TABLE public.conf_mail_null OWNER TO postgres;

--
-- TOC entry 235 (class 1259 OID 17345)
-- Name: mail; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.mail (
    ordid text,
    dop integer,
    koor character varying(15),
    order_no character varying(15),
    line_no character varying(5),
    rel_no character varying(5),
    part_no character varying(25),
    descr character varying(150),
    country character varying(3),
    prom_date date,
    prom_week integer,
    load_id bigint,
    ship_date date,
    date_entered timestamp without time zone,
    cust_id uuid NOT NULL,
    prod date,
    prod_week integer,
    planner_buyer character varying(500),
    indeks character varying(500),
    opis character varying(1000),
    typ_zdarzenia character varying(1000),
    status_informacji character varying(500),
    zest character varying(50),
    info_handlo boolean,
    logistyka boolean,
    seria0 boolean,
    data0 date,
    cust_line_stat character varying(50),
    ord_objver timestamp without time zone
);


ALTER TABLE public.mail OWNER TO postgres;

--
-- TOC entry 236 (class 1259 OID 17350)
-- Name: send_mail; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.send_mail (
    mail character varying(250) NOT NULL,
    typ character varying(150),
    typ_zdarzenia character varying(500),
    status_informacji character varying(500),
    info character varying(500),
    corr character varying(15),
    cust_ord character varying(15),
    c_lin character varying(5),
    c_rel character varying(5),
    catalog_desc character varying(150),
    c_ry character varying(10),
    load_id character varying(10),
    ship_date date,
    prom_week integer,
    prod_week integer,
    prod_date date,
    part_buyer character varying(250),
    shortage_part character varying(500),
    short_nam character varying(1000),
    dop integer,
    created timestamp without time zone,
    last_mail timestamp without time zone
);


ALTER TABLE public.send_mail OWNER TO postgres;

--
-- TOC entry 237 (class 1259 OID 17355)
-- Name: confirm_ord; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.confirm_ord AS
 SELECT a.order_no,
    a.cust_no,
    a.reference,
    a.addr1,
    max(a.date_entered) AS date1,
    max(a.objversion) AS date2,
    sum(a.gotowe) AS sum,
    a.country
   FROM ( SELECT a_1.koor,
            a_1.order_no,
            a_1.line_no,
            a_1.rel_no,
            a_1.line_item_no,
            a_1.last_mail_conf,
            a_1.state_conf,
            a_1.line_state,
            a_1.cust_order_state,
            a_1.country,
            a_1.cust_no,
            a_1.zip_code,
            a_1.addr1,
            a_1.prom_date,
            a_1.prom_week,
            a_1.load_id,
            a_1.ship_date,
            a_1.part_no,
            a_1.descr,
            a_1.configuration,
            a_1.buy_qty_due,
            a_1.desired_qty,
            a_1.qty_invoiced,
            a_1.qty_shipped,
            a_1.qty_assigned,
            a_1.dop_connection_db,
            a_1.dop_id,
            a_1.dop_state,
            a_1.data_dop,
            a_1.dop_qty,
            a_1.dop_made,
            a_1.date_entered,
            a_1.chksum,
            a_1.custid,
            a_1.id,
            a_1.zest,
            a_1.seria0,
            a_1.data0,
            a_1.objversion,
            public.get_refer(a_1.addr1) AS reference,
                CASE
                    WHEN (((a_1.dop_connection_db)::text = 'AUT'::text) AND (a_1.dop_state IS NULL)) THEN 1
                    ELSE 0
                END AS gotowe
           FROM (((public.cust_ord a_1
             LEFT JOIN ( SELECT send_mail.cust_ord
                   FROM public.send_mail
                  WHERE ((send_mail.typ)::text = 'NIE POTWIERDZAĆ'::text)
                  GROUP BY send_mail.cust_ord) b ON (((b.cust_ord)::text = (a_1.order_no)::text)))
             LEFT JOIN ( SELECT mail.order_no
                   FROM public.mail
                  WHERE ((mail.info_handlo = true) OR ((mail.status_informacji)::text = 'POPRAWIĆ'::text))
                  GROUP BY mail.order_no) g ON (((g.order_no)::text = (a_1.order_no)::text)))
             LEFT JOIN ( SELECT cust_ord.order_no
                   FROM public.cust_ord
                  WHERE (((cust_ord.state_conf)::text = 'Wydrukow.'::text) AND (cust_ord.last_mail_conf IS NOT NULL))
                  GROUP BY cust_ord.order_no) c ON (((c.order_no)::text = (a_1.order_no)::text)))
          WHERE ((b.cust_ord IS NULL) AND (g.order_no IS NULL) AND (((a_1.state_conf)::text = 'Nie wydruk.'::text) OR (a_1.last_mail_conf IS NULL)) AND (public.is_refer(a_1.addr1) = true) AND ("substring"((a_1.order_no)::text, 1, 1) = 'S'::text) AND ((a_1.cust_order_state)::text <> ALL (ARRAY[('Częściowo dostarczone'::character varying)::text, ('Zaplanowane'::character varying)::text])) AND ("substring"((a_1.part_no)::text, 1, 3) <> ALL (ARRAY['633'::text, '628'::text, '1K1'::text, '1U2'::text, '632'::text])) AND (((c.order_no IS NOT NULL) AND ((a_1.dop_connection_db)::text <> 'MAN'::text)) OR (c.order_no IS NULL)))) a
  GROUP BY a.order_no, a.cust_no, a.reference, a.addr1, a.country
 HAVING ((sum(a.gotowe) = 0) AND (COALESCE((max(a.date_entered))::timestamp with time zone, (now() - '03:00:00'::interval)) < (now() - '02:00:00'::interval)) AND (max(a.objversion) < (now() - '02:00:00'::interval)));


ALTER TABLE public.confirm_ord OWNER TO postgres;

--
-- TOC entry 259 (class 1259 OID 17954)
-- Name: contract_calendar; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.contract_calendar (
    contract character varying(15) NOT NULL,
    calendar_id character varying(15) NOT NULL
);


ALTER TABLE public.contract_calendar OWNER TO postgres;

--
-- TOC entry 238 (class 1259 OID 17360)
-- Name: cust_odr_conf_mod; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.cust_odr_conf_mod AS
 SELECT b.koor,
    b.order_no,
    b.line_no,
    b.rel_no,
    b.part_no,
    b.descr,
    a.dop_id,
    b.configuration AS new_configuration,
    a.configuration AS old_configuration,
    a.date_add
   FROM public.cust_ord b,
    ( SELECT cust_ord_history.koor,
            cust_ord_history.order_no,
            cust_ord_history.line_no,
            cust_ord_history.rel_no,
            cust_ord_history.line_item_no,
            cust_ord_history.customer_po_line_no,
            cust_ord_history.dimmension,
            cust_ord_history.last_mail_conf,
            cust_ord_history.state_conf,
            cust_ord_history.line_state,
            cust_ord_history.cust_order_state,
            cust_ord_history.country,
            cust_ord_history.cust_no,
            cust_ord_history.zip_code,
            cust_ord_history.addr1,
            cust_ord_history.prom_date,
            cust_ord_history.prom_week,
            cust_ord_history.load_id,
            cust_ord_history.ship_date,
            cust_ord_history.part_no,
            cust_ord_history.descr,
            cust_ord_history.configuration,
            cust_ord_history.buy_qty_due,
            cust_ord_history.desired_qty,
            cust_ord_history.qty_invoiced,
            cust_ord_history.qty_shipped,
            cust_ord_history.qty_assigned,
            cust_ord_history.dop_connection_db,
            cust_ord_history.dop_id,
            cust_ord_history.dop_state,
            cust_ord_history.data_dop,
            cust_ord_history.dop_qty,
            cust_ord_history.dop_made,
            cust_ord_history.date_entered,
            cust_ord_history.chksum,
            cust_ord_history.custid,
            cust_ord_history.id,
            cust_ord_history.zest,
            cust_ord_history.seria0,
            cust_ord_history.data0,
            cust_ord_history.objversion,
            cust_ord_history.operation,
            cust_ord_history.date_add
           FROM public.cust_ord_history
          WHERE ((cust_ord_history.operation)::text ~~ '%Configuration;%'::text)) a
  WHERE ((a.id = b.id) AND ((a.configuration)::text <> '*'::text))
  ORDER BY a.date_add DESC;


ALTER TABLE public.cust_odr_conf_mod OWNER TO postgres;

--
-- TOC entry 239 (class 1259 OID 17365)
-- Name: cust_ord_mod_confirm_date; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.cust_ord_mod_confirm_date AS
 SELECT b.koor,
    b.order_no,
    b.line_no,
    b.rel_no,
    b.part_no,
    b.descr,
    b.dop_id,
    b.prom_date AS new_prom_date,
    a.prom_date AS old_prom_date,
    b.data_dop AS new_prod_date,
    a.data_dop AS old_prod_date,
    a.date_add
   FROM public.cust_ord b,
    ( SELECT cust_ord_history.koor,
            cust_ord_history.order_no,
            cust_ord_history.line_no,
            cust_ord_history.rel_no,
            cust_ord_history.line_item_no,
            cust_ord_history.customer_po_line_no,
            cust_ord_history.dimmension,
            cust_ord_history.last_mail_conf,
            cust_ord_history.state_conf,
            cust_ord_history.line_state,
            cust_ord_history.cust_order_state,
            cust_ord_history.country,
            cust_ord_history.cust_no,
            cust_ord_history.zip_code,
            cust_ord_history.addr1,
            cust_ord_history.prom_date,
            cust_ord_history.prom_week,
            cust_ord_history.load_id,
            cust_ord_history.ship_date,
            cust_ord_history.part_no,
            cust_ord_history.descr,
            cust_ord_history.configuration,
            cust_ord_history.buy_qty_due,
            cust_ord_history.desired_qty,
            cust_ord_history.qty_invoiced,
            cust_ord_history.qty_shipped,
            cust_ord_history.qty_assigned,
            cust_ord_history.dop_connection_db,
            cust_ord_history.dop_id,
            cust_ord_history.dop_state,
            cust_ord_history.data_dop,
            cust_ord_history.dop_qty,
            cust_ord_history.dop_made,
            cust_ord_history.date_entered,
            cust_ord_history.chksum,
            cust_ord_history.custid,
            cust_ord_history.id,
            cust_ord_history.zest,
            cust_ord_history.seria0,
            cust_ord_history.data0,
            cust_ord_history.objversion,
            cust_ord_history.operation,
            cust_ord_history.date_add
           FROM public.cust_ord_history
          WHERE ((cust_ord_history.operation)::text ~~ '%Confirm_date;%'::text)) a
  WHERE ((a.id = b.id) AND (a.prom_date > b.prom_date))
  ORDER BY a.date_add DESC;


ALTER TABLE public.cust_ord_mod_confirm_date OWNER TO postgres;

--
-- TOC entry 240 (class 1259 OID 17370)
-- Name: cust_ord_mod_ship_date; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.cust_ord_mod_ship_date WITH (security_barrier='false') AS
 SELECT b.koor,
    b.order_no,
    b.line_no,
    b.rel_no,
    b.part_no,
    b.descr,
    b.ship_date AS new_ship_date,
    a.ship_date AS old_ship_date,
    a.date_add
   FROM public.cust_ord b,
    ( SELECT cust_ord_history.koor,
            cust_ord_history.order_no,
            cust_ord_history.line_no,
            cust_ord_history.rel_no,
            cust_ord_history.line_item_no,
            cust_ord_history.customer_po_line_no,
            cust_ord_history.dimmension,
            cust_ord_history.last_mail_conf,
            cust_ord_history.state_conf,
            cust_ord_history.line_state,
            cust_ord_history.cust_order_state,
            cust_ord_history.country,
            cust_ord_history.cust_no,
            cust_ord_history.zip_code,
            cust_ord_history.addr1,
            cust_ord_history.prom_date,
            cust_ord_history.prom_week,
            cust_ord_history.load_id,
            cust_ord_history.ship_date,
            cust_ord_history.part_no,
            cust_ord_history.descr,
            cust_ord_history.configuration,
            cust_ord_history.buy_qty_due,
            cust_ord_history.desired_qty,
            cust_ord_history.qty_invoiced,
            cust_ord_history.qty_shipped,
            cust_ord_history.qty_assigned,
            cust_ord_history.dop_connection_db,
            cust_ord_history.dop_id,
            cust_ord_history.dop_state,
            cust_ord_history.data_dop,
            cust_ord_history.dop_qty,
            cust_ord_history.dop_made,
            cust_ord_history.date_entered,
            cust_ord_history.chksum,
            cust_ord_history.custid,
            cust_ord_history.id,
            cust_ord_history.zest,
            cust_ord_history.seria0,
            cust_ord_history.data0,
            cust_ord_history.objversion,
            cust_ord_history.operation,
            cust_ord_history.date_add
           FROM public.cust_ord_history
          WHERE ((cust_ord_history.operation)::text ~~ '%Shipment_date;%'::text)) a
  WHERE (a.id = b.id)
  ORDER BY a.date_add DESC;


ALTER TABLE public.cust_ord_mod_ship_date OWNER TO postgres;

--
-- TOC entry 241 (class 1259 OID 17375)
-- Name: data; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.data (
    indeks character varying(25),
    opis character varying(150),
    kolekcja character varying(40),
    mag double precision,
    planner_buyer character varying(10),
    rodzaj character varying(15),
    czas_dostawy double precision,
    data_gwarancji date,
    data_dost date NOT NULL,
    wlk_dost double precision,
    bilans double precision,
    data_braku date,
    "bil_dost_dzień" double precision,
    typ_zdarzenia character varying(35),
    widoczny_od_dnia timestamp without time zone,
    sum_dost double precision,
    sum_potrz double precision,
    "sum_dost_opóźnion" double precision,
    "sum_potrz_opóźnion" double precision,
    status_informacji character varying(35),
    refr_date timestamp without time zone,
    id uuid NOT NULL,
    chk double precision,
    przyczyna integer,
    informacja character varying(250),
    umiejsc character varying(20)
);


ALTER TABLE public.data OWNER TO postgres;

--
-- TOC entry 242 (class 1259 OID 17380)
-- Name: datatbles; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.datatbles (
    table_name character varying(30) NOT NULL,
    last_modify timestamp without time zone,
    start_update timestamp without time zone,
    in_progress boolean,
    updt_errors boolean
);


ALTER TABLE public.datatbles OWNER TO postgres;

--
-- TOC entry 243 (class 1259 OID 17383)
-- Name: day_qty_ifs; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.day_qty_ifs (
    work_day date,
    typ character varying(3),
    wrkc character varying(10),
    next_wrkc character varying(10),
    qty_all double precision,
    id character varying(100) NOT NULL
);


ALTER TABLE public.day_qty_ifs OWNER TO postgres;

--
-- TOC entry 244 (class 1259 OID 17386)
-- Name: demands_view; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.demands_view AS
 SELECT demands.part_no,
    demands.work_day,
    demands.expected_leadtime,
    public.date_fromnow((demands.expected_leadtime)::integer) AS date_fromnow,
    demands.purch_qty,
    demands.qty_demand,
    public.dmd_type((demands.type_dmd)::integer) AS dmd_type,
    demands.balance,
    demands.bal_stock,
    demands.koor,
    demands.type,
    demands.dat_shortage,
    demands.objversion
   FROM public.demands
  ORDER BY demands.part_no, demands.work_day;


ALTER TABLE public.demands_view OWNER TO postgres;

--
-- TOC entry 245 (class 1259 OID 17390)
-- Name: potw; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.potw (
    indeks character varying(15),
    dost_ilosc double precision,
    data_dost date,
    sum_dost double precision,
    rodzaj_potw character varying(50),
    termin_wazn date,
    koor character varying(8),
    date_created timestamp without time zone,
    id uuid NOT NULL,
    info character varying(500),
    umiejsc character varying(20)
);


ALTER TABLE public.potw OWNER TO postgres;

--
-- TOC entry 246 (class 1259 OID 17395)
-- Name: info_inpotw; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.info_inpotw WITH (security_barrier='false') AS
 SELECT a.ordid,
    a.zest,
    a.cust_id,
    b.id,
    b.data_dost,
    a.date_reuired AS date_required,
    a.prod_date,
    b.rodzaj_potw,
    (
        CASE
            WHEN (b.info IS NOT NULL) THEN ((('Indeks:'::text || (b.indeks)::text) || ':'::text) || (b.info)::text)
            ELSE NULL::text
        END)::character varying(500) AS info
   FROM public.braki a,
    public.potw b
  WHERE (((b.indeks)::text = (a.indeks)::text) AND
        CASE
            WHEN ((b.rodzaj_potw)::text <> 'NIE ZAMAWIAM'::text) THEN (b.data_dost = a.data_dost)
            ELSE (b.data_dost IS NOT NULL)
        END);


ALTER TABLE public.info_inpotw OWNER TO postgres;

--
-- TOC entry 247 (class 1259 OID 17400)
-- Name: kontakty; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.kontakty (
    country_coor character varying(100),
    ifs_user character varying(250),
    mail character varying(500),
    id uuid NOT NULL
);


ALTER TABLE public.kontakty OWNER TO postgres;

--
-- TOC entry 248 (class 1259 OID 17405)
-- Name: fill_sendmail; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.fill_sendmail WITH (security_barrier='false') AS
 SELECT a.mail,
    a.typ,
    a.typ_zdarzenia,
    a.status_informacji,
    b.info,
    a.koor AS corr,
    a.order_no AS cust_ord,
    a.line_no AS c_lin,
    a.rel_no AS c_rel,
    a.descr AS catalog_desc,
    a.country AS c_ry,
    (a.load_id)::character varying AS load_id,
    a.ship_date,
    a.prom_week,
    a.prod_week,
    a.prod AS prod_date,
    a.planner_buyer AS part_buyer,
    a.indeks AS shortage_part,
    a.opis AS short_nam,
    a.dop,
    a.date_entered AS created,
    (('now'::text)::date)::timestamp without time zone AS last_mail
   FROM (( SELECT a_1.mail,
            a_1.cust_id,
            a_1.koor,
            a_1.order_no,
            a_1.line_no,
            a_1.rel_no,
            a_1.descr,
            a_1.country,
            a_1.load_id,
            a_1.ship_date,
            a_1.prom_week,
            a_1.prod_week,
            a_1.prod,
            a_1.planner_buyer,
            a_1.indeks,
            a_1.opis,
            a_1.dop,
            a_1.date_entered,
            a_1.typ,
            a_1.typ_zdarzenia,
            a_1.status_informacji
           FROM ( SELECT b_1.mail,
                    a_2.cust_id,
                    a_2.koor,
                    a_2.order_no,
                    a_2.line_no,
                    a_2.rel_no,
                    a_2.descr,
                    a_2.country,
                    a_2.load_id,
                    a_2.ship_date,
                    a_2.prom_week,
                    a_2.prod_week,
                    a_2.prod,
                    a_2.planner_buyer,
                    a_2.indeks,
                    a_2.opis,
                    a_2.dop,
                    a_2.date_entered,
                    'MAIL'::text AS typ,
                    a_2.typ_zdarzenia,
                    a_2.status_informacji
                   FROM ( SELECT mail.ordid,
                            mail.dop,
                            mail.koor,
                            mail.order_no,
                            mail.line_no,
                            mail.rel_no,
                            mail.part_no,
                            mail.descr,
                            ((mail.country)::text ||
                                CASE
                                    WHEN (public.is_interncl(mail.cust_id) = true) THEN '_SITS'::text
                                    ELSE ''::text
                                END) AS country,
                            mail.prom_date,
                            mail.prom_week,
                            mail.load_id,
                            mail.ship_date,
                            mail.date_entered,
                            mail.cust_id,
                            mail.prod,
                            mail.prod_week,
                            mail.planner_buyer,
                            mail.indeks,
                            mail.opis,
                            mail.typ_zdarzenia,
                            mail.status_informacji,
                            mail.zest,
                            mail.info_handlo,
                            mail.logistyka,
                            mail.seria0,
                            mail.data0,
                            mail.cust_line_stat,
                            mail.ord_objver
                           FROM (public.mail mail
                             LEFT JOIN ( SELECT mail_hist.cust_id,
                                    max(mail_hist.date_addd) AS max
                                   FROM public.mail_hist
                                  GROUP BY mail_hist.cust_id
                                 HAVING (max(mail_hist.date_addd) > (now() - '00:10:00'::interval))) a_3 ON ((a_3.cust_id = mail.cust_id)))
                          WHERE (((a_3.cust_id IS NULL) AND (public.is_for_mail(mail.status_informacji) = true) AND (mail.info_handlo = true)) OR ((mail.status_informacji)::text = 'POPRAWIĆ'::text))) a_2,
                    public.kontakty b_1
                  WHERE ((b_1.ifs_user)::text = (a_2.koor)::text)) a_1
        UNION
         SELECT b_1.mail,
            a_1.cust_id,
            a_1.koor,
            a_1.order_no,
            a_1.line_no,
            a_1.rel_no,
            a_1.descr,
            a_1.country,
            a_1.load_id,
            a_1.ship_date,
            a_1.prom_week,
            a_1.prod_week,
            a_1.prod,
            a_1.planner_buyer,
            a_1.indeks,
            a_1.opis,
            a_1.dop,
            a_1.date_entered,
            'MAIL'::text AS typ,
            a_1.typ_zdarzenia,
            a_1.status_informacji
           FROM ( SELECT mail.ordid,
                    mail.dop,
                    mail.koor,
                    mail.order_no,
                    mail.line_no,
                    mail.rel_no,
                    mail.part_no,
                    mail.descr,
                    ((mail.country)::text ||
                        CASE
                            WHEN (public.is_interncl(mail.cust_id) = true) THEN '_SITS'::text
                            ELSE ''::text
                        END) AS country,
                    mail.prom_date,
                    mail.prom_week,
                    mail.load_id,
                    mail.ship_date,
                    mail.date_entered,
                    mail.cust_id,
                    mail.prod,
                    mail.prod_week,
                    mail.planner_buyer,
                    mail.indeks,
                    mail.opis,
                    mail.typ_zdarzenia,
                    mail.status_informacji,
                    mail.zest,
                    mail.info_handlo,
                    mail.logistyka,
                    mail.seria0,
                    mail.data0,
                    mail.cust_line_stat,
                    mail.ord_objver
                   FROM (public.mail mail
                     LEFT JOIN ( SELECT mail_hist.cust_id,
                            max(mail_hist.date_addd) AS max
                           FROM public.mail_hist
                          GROUP BY mail_hist.cust_id
                         HAVING (max(mail_hist.date_addd) > (now() - '00:10:00'::interval))) a_2 ON ((a_2.cust_id = mail.cust_id)))
                  WHERE (((a_2.cust_id IS NULL) AND (public.is_for_mail(mail.status_informacji) = true) AND (mail.info_handlo = true)) OR ((mail.status_informacji)::text = 'POPRAWIĆ'::text))) a_1,
            public.kontakty b_1
          WHERE ((b_1.country_coor)::text =
                CASE
                    WHEN ("substring"((a_1.order_no)::text, 1, 1) = 'Z'::text) THEN ('Z'::character varying)::text
                    ELSE a_1.country
                END)
        UNION
         SELECT b_1.mail,
            a_1.cust_id,
            a_1.koor,
            a_1.order_no,
            a_1.line_no,
            a_1.rel_no,
            a_1.descr,
            a_1.country,
            a_1.load_id,
            a_1.ship_date,
            a_1.prom_week,
            a_1.prod_week,
            a_1.prod,
            a_1.planner_buyer,
            a_1.indeks,
            a_1.opis,
            a_1.dop,
            a_1.date_entered,
            'Seria Zero'::text AS typ,
            a_1.typ_zdarzenia,
            a_1.status_informacji
           FROM ( SELECT mail.ordid,
                    mail.dop,
                    mail.koor,
                    mail.order_no,
                    mail.line_no,
                    mail.rel_no,
                    mail.part_no,
                    mail.descr,
                    ((mail.country)::text ||
                        CASE
                            WHEN (public.is_interncl(mail.cust_id) = true) THEN '_SITS'::text
                            ELSE ''::text
                        END) AS country,
                    mail.prom_date,
                    mail.prom_week,
                    mail.load_id,
                    mail.ship_date,
                    mail.date_entered,
                    mail.cust_id,
                    mail.prod,
                    mail.prod_week,
                    mail.planner_buyer,
                    mail.indeks,
                    mail.opis,
                    mail.typ_zdarzenia,
                    mail.status_informacji,
                    mail.zest,
                    mail.info_handlo,
                    mail.logistyka,
                    mail.seria0,
                    mail.data0,
                    mail.cust_line_stat,
                    mail.ord_objver
                   FROM (public.mail mail
                     LEFT JOIN ( SELECT mail_hist.cust_id,
                            max(mail_hist.date_addd) AS max
                           FROM public.mail_hist
                          GROUP BY mail_hist.cust_id
                         HAVING (max(mail_hist.date_addd) > (now() - '00:10:00'::interval))) a_2 ON ((a_2.cust_id = mail.cust_id)))
                  WHERE ((a_2.cust_id IS NULL) AND (((mail.indeks)::text = 'Seria Zero '::text) OR ((public.is_seria0(mail.cust_id) = true) AND (public.is_for_mail(mail.status_informacji) = true))) AND ((mail.info_handlo = true) OR ((mail.data0 < mail.prod) AND (mail.seria0 = true))))) a_1,
            public.kontakty b_1
          WHERE ((b_1.ifs_user)::text = (a_1.koor)::text)
        UNION
         SELECT b_1.mail,
            a_1.cust_id,
            a_1.koor,
            a_1.order_no,
            a_1.line_no,
            a_1.rel_no,
            a_1.descr,
            a_1.country,
            a_1.load_id,
            a_1.ship_date,
            a_1.prom_week,
            a_1.prod_week,
            a_1.prod,
            a_1.planner_buyer,
            a_1.indeks,
            a_1.opis,
            a_1.dop,
            a_1.date_entered,
            'Seria Zero'::text AS typ,
            a_1.typ_zdarzenia,
            a_1.status_informacji
           FROM ( SELECT mail.ordid,
                    mail.dop,
                    mail.koor,
                    mail.order_no,
                    mail.line_no,
                    mail.rel_no,
                    mail.part_no,
                    mail.descr,
                    ((mail.country)::text ||
                        CASE
                            WHEN (public.is_interncl(mail.cust_id) = true) THEN '_SITS'::text
                            ELSE ''::text
                        END) AS country,
                    mail.prom_date,
                    mail.prom_week,
                    mail.load_id,
                    mail.ship_date,
                    mail.date_entered,
                    mail.cust_id,
                    mail.prod,
                    mail.prod_week,
                    mail.planner_buyer,
                    mail.indeks,
                    mail.opis,
                    mail.typ_zdarzenia,
                    mail.status_informacji,
                    mail.zest,
                    mail.info_handlo,
                    mail.logistyka,
                    mail.seria0,
                    mail.data0,
                    mail.cust_line_stat,
                    mail.ord_objver
                   FROM (public.mail mail
                     LEFT JOIN ( SELECT mail_hist.cust_id,
                            max(mail_hist.date_addd) AS max
                           FROM public.mail_hist
                          GROUP BY mail_hist.cust_id
                         HAVING (max(mail_hist.date_addd) > (now() - '00:10:00'::interval))) a_2 ON ((a_2.cust_id = mail.cust_id)))
                  WHERE ((a_2.cust_id IS NULL) AND (((mail.indeks)::text = 'Seria Zero '::text) OR ((public.is_seria0(mail.cust_id) = true) AND (public.is_for_mail(mail.status_informacji) = true))) AND ((mail.info_handlo = true) OR ((mail.data0 < mail.prod) AND (mail.seria0 = true))))) a_1,
            public.kontakty b_1
          WHERE (((b_1.country_coor)::text =
                CASE
                    WHEN ("substring"((a_1.order_no)::text, 1, 1) = 'Z'::text) THEN ('Z'::character varying)::text
                    ELSE a_1.country
                END) OR ((b_1.country_coor)::text = 'Seria Zero'::text))
        UNION
         SELECT b_1.mail,
            a_1.cust_id,
            a_1.koor,
            a_1.order_no,
            a_1.line_no,
            a_1.rel_no,
            a_1.descr,
            a_1.country,
            a_1.load_id,
            a_1.ship_date,
            a_1.prom_week,
            a_1.prod_week,
            public.date_shift_days(a_1.prod, 1) AS prod,
            a_1.planner_buyer,
            a_1.indeks,
            a_1.opis,
            a_1.dop,
            a_1.date_entered,
            'MAIL LOG'::text AS typ,
            a_1.typ_zdarzenia,
            a_1.status_informacji
           FROM ( SELECT mail.ordid,
                    mail.dop,
                    mail.koor,
                    mail.order_no,
                    mail.line_no,
                    mail.rel_no,
                    mail.part_no,
                    mail.descr,
                    ((mail.country)::text ||
                        CASE
                            WHEN (public.is_interncl(mail.cust_id) = true) THEN '_SITS'::text
                            ELSE ''::text
                        END) AS country,
                    mail.prom_date,
                    mail.prom_week,
                    mail.load_id,
                    mail.ship_date,
                    mail.date_entered,
                    mail.cust_id,
                    mail.prod,
                    mail.prod_week,
                    mail.planner_buyer,
                    mail.indeks,
                    mail.opis,
                    mail.typ_zdarzenia,
                    mail.status_informacji,
                    mail.zest,
                    mail.info_handlo,
                    mail.logistyka,
                    mail.seria0,
                    mail.data0,
                    mail.cust_line_stat,
                    mail.ord_objver
                   FROM (public.mail mail
                     LEFT JOIN ( SELECT mail_hist.cust_id,
                            max(mail_hist.date_addd) AS max
                           FROM public.mail_hist
                          GROUP BY mail_hist.cust_id
                         HAVING (max(mail_hist.date_addd) > (now() - '00:10:00'::interval))) a_2 ON ((a_2.cust_id = mail.cust_id)))
                  WHERE (((a_2.cust_id IS NULL) AND (public.is_confirm(mail.status_informacji) = true) AND (mail.logistyka = true)) OR ((public.is_alter(mail.status_informacji) = true) AND (mail.prom_week = mail.prod_week) AND (mail.logistyka = true)))) a_1,
            public.kontakty b_1
          WHERE ((b_1.country_coor)::text = 'L'::text)
        UNION
         SELECT b_1.mail,
            a_1.cust_id,
            a_1.koor,
            a_1.order_no,
            a_1.line_no,
            a_1.rel_no,
            a_1.descr,
            a_1.country,
            a_1.load_id,
            a_1.ship_date,
            a_1.prom_week,
            a_1.prod_week,
            a_1.prod,
            a_1.planner_buyer,
            a_1.indeks,
            a_1.opis,
            a_1.dop,
            a_1.date_entered,
            a_1.typ,
            a_1.typ_zdarzenia,
            a_1.status_informacji
           FROM ( SELECT a_2.id AS cust_id,
                    a_2.koor,
                    a_2.order_no,
                    a_2.line_no,
                    a_2.rel_no,
                    a_2.descr,
                    a_2.country,
                    a_2.load_id,
                    a_2.ship_date,
                    (a_2.prom_week)::integer AS prom_week,
                    COALESCE(b_2.prod_week, (to_char(
                        CASE
                            WHEN ("substring"((a_2.order_no)::text, 1, 1) = 'Z'::text) THEN (to_date((a_2.prom_week)::text, 'iyyyiw'::text) + '6 days'::interval)
                            ELSE (((to_date((a_2.prom_week)::text, 'iyyyiw'::text) + public.shipment_day((a_2.country)::character varying, a_2.cust_no, a_2.zip_code, a_2.addr1)) - 1))::timestamp without time zone
                        END, 'iyyyiw'::text))::integer) AS prod_week,
                    COALESCE(b_2.prod, a_2.data_dop) AS prod,
                    COALESCE(b_2.planner_buyer, a_2.koor) AS planner_buyer,
                    COALESCE(b_2.indeks,
                        CASE
                            WHEN ((a_2.cust_order_state)::text = ANY (ARRAY[('Zaplanowane'::character varying)::text, ('Zablok. kredyt'::character varying)::text])) THEN a_2.cust_order_state
                            ELSE 'BŁĄD POTW.'::character varying
                        END) AS indeks,
                    COALESCE(b_2.opis, (
                        CASE
                            WHEN ((a_2.cust_order_state)::text = ANY (ARRAY[('Zaplanowane'::character varying)::text, ('Zablok. kredyt'::character varying)::text])) THEN 'Status zamówienia klienta'::text
                            ELSE 'Data realizacji późniejsza od daty obiecanej'::text
                        END)::character varying) AS opis,
                    COALESCE(b_2.dop, a_2.dop_id) AS dop,
                    COALESCE(b_2.date_entered, a_2.date_entered, a_2.objversion) AS date_entered,
                    'NIE POTWIERDZAĆ'::character varying AS typ,
                    b_2.typ_zdarzenia,
                    b_2.status_informacji
                   FROM (( SELECT cust_ord.koor,
                            cust_ord.order_no,
                            cust_ord.line_no,
                            cust_ord.rel_no,
                            cust_ord.line_item_no,
                            cust_ord.last_mail_conf,
                            cust_ord.state_conf,
                            cust_ord.line_state,
                            cust_ord.cust_order_state,
                            ((cust_ord.country)::text ||
                                CASE
                                    WHEN (public.is_interncl(cust_ord.id) = true) THEN '_SITS'::text
                                    ELSE ''::text
                                END) AS country,
                            cust_ord.cust_no,
                            cust_ord.zip_code,
                            cust_ord.addr1,
                            cust_ord.prom_date,
                            cust_ord.prom_week,
                            cust_ord.load_id,
                            cust_ord.ship_date,
                            cust_ord.part_no,
                            cust_ord.descr,
                            cust_ord.configuration,
                            cust_ord.buy_qty_due,
                            cust_ord.desired_qty,
                            cust_ord.qty_invoiced,
                            cust_ord.qty_shipped,
                            cust_ord.qty_assigned,
                            cust_ord.dop_connection_db,
                            cust_ord.dop_id,
                            cust_ord.dop_state,
                            cust_ord.data_dop,
                            cust_ord.dop_qty,
                            cust_ord.dop_made,
                            cust_ord.date_entered,
                            cust_ord.chksum,
                            cust_ord.custid,
                            cust_ord.id,
                            cust_ord.zest,
                            cust_ord.seria0,
                            cust_ord.data0,
                            cust_ord.objversion
                           FROM public.cust_ord
                          WHERE (("substring"((cust_ord.part_no)::text, 1, 3) <> ALL (ARRAY['633'::text, '628'::text, '1K1'::text, '1U2'::text, '632'::text])) AND ((cust_ord.order_no)::text IN ( SELECT a_3.order_no
                                   FROM ( SELECT a_1_1.koor,
    a_1_1.order_no,
    a_1_1.line_no,
    a_1_1.rel_no,
    a_1_1.line_item_no,
    a_1_1.last_mail_conf,
    a_1_1.state_conf,
    a_1_1.line_state,
    a_1_1.cust_order_state,
    a_1_1.country,
    a_1_1.cust_no,
    a_1_1.zip_code,
    a_1_1.addr1,
    a_1_1.prom_date,
    a_1_1.prom_week,
    a_1_1.load_id,
    a_1_1.ship_date,
    a_1_1.part_no,
    a_1_1.descr,
    a_1_1.configuration,
    a_1_1.buy_qty_due,
    a_1_1.desired_qty,
    a_1_1.qty_invoiced,
    a_1_1.qty_shipped,
    a_1_1.qty_assigned,
    a_1_1.dop_connection_db,
    a_1_1.dop_id,
    a_1_1.dop_state,
    a_1_1.data_dop,
    a_1_1.dop_qty,
    a_1_1.dop_made,
    a_1_1.date_entered,
    a_1_1.chksum,
    a_1_1.custid,
    a_1_1.id,
    a_1_1.zest,
    a_1_1.seria0,
    a_1_1.data0,
    a_1_1.objversion,
    public.get_refer(a_1_1.addr1) AS reference,
  CASE
   WHEN (((a_1_1.dop_connection_db)::text = 'AUT'::text) AND (a_1_1.dop_state IS NULL)) THEN 1
   ELSE 0
  END AS gotowe
   FROM (public.cust_ord a_1_1
     LEFT JOIN ( SELECT cust_ord_1.order_no
     FROM public.cust_ord cust_ord_1
    WHERE (((cust_ord_1.state_conf)::text = 'Wydrukow.'::text) AND (cust_ord_1.last_mail_conf IS NOT NULL))
    GROUP BY cust_ord_1.order_no) c ON (((c.order_no)::text = (a_1_1.order_no)::text)))
  WHERE ((((a_1_1.state_conf)::text = 'Nie wydruk.'::text) OR (a_1_1.last_mail_conf IS NULL)) AND (public.is_refer(a_1_1.addr1) = true) AND ("substring"((a_1_1.order_no)::text, 1, 1) = 'S'::text) AND ((a_1_1.cust_order_state)::text <> ALL (ARRAY[('Częściowo dostarczone'::character varying)::text, ('Zablok. kredyt'::character varying)::text, 'Wydane'::text, ('Zaplanowane'::character varying)::text])) AND ("substring"((a_1_1.part_no)::text, 1, 3) <> ALL (ARRAY['633'::text, '628'::text, '1K1'::text, '1U2'::text, '632'::text])) AND (((c.order_no IS NOT NULL) AND ((a_1_1.dop_connection_db)::text <> 'MAN'::text)) OR (c.order_no IS NULL)))) a_3
                                  GROUP BY a_3.order_no, a_3.cust_no, a_3.reference, a_3.addr1, a_3.country
                                 HAVING ((sum(a_3.gotowe) = 0) AND (max(a_3.objversion) < (now() - '02:00:00'::interval))))))) a_2
                     LEFT JOIN ( SELECT mail.ordid,
                            mail.dop,
                            mail.koor,
                            mail.order_no,
                            mail.line_no,
                            mail.rel_no,
                            mail.part_no,
                            mail.descr,
                            ((mail.country)::text ||
                                CASE
                                    WHEN (public.is_interncl(mail.cust_id) = true) THEN '_SITS'::text
                                    ELSE ''::text
                                END) AS country,
                            mail.prom_date,
                            mail.prom_week,
                            mail.load_id,
                            mail.ship_date,
                            mail.date_entered,
                            mail.cust_id,
                            mail.prod,
                            mail.prod_week,
                            mail.planner_buyer,
                            mail.indeks,
                            mail.opis,
                            mail.typ_zdarzenia,
                            mail.status_informacji,
                            mail.zest,
                            mail.info_handlo,
                            mail.logistyka,
                            mail.seria0,
                            mail.data0,
                            mail.cust_line_stat,
                            mail.ord_objver
                           FROM public.mail
                          WHERE ((mail.info_handlo = true) OR (mail.logistyka = true))) b_2 ON ((b_2.cust_id = a_2.id)))
                  WHERE ((b_2.cust_id IS NOT NULL) OR (((a_2.cust_order_state)::text <> ALL (ARRAY[('Aktywowane'::character varying)::text, ('Częściowo dostarczone'::character varying)::text, ('Zarezerwowane'::character varying)::text])) AND ((a_2.line_state)::text = 'Aktywowana'::text) AND (upper("substring"((a_2.cust_no)::text, 1, 4)) <> 'IKEA'::text) AND ("substring"((a_2.addr1)::text, 1, 4) <> 'IKEA'::text)) OR ((a_2.data_dop >
                        CASE
                            WHEN ("substring"((a_2.order_no)::text, 1, 1) = 'Z'::text) THEN (to_date((a_2.prom_week)::text, 'iyyyiw'::text) + '6 days'::interval)
                            ELSE (((to_date((a_2.prom_week)::text, 'iyyyiw'::text) + public.shipment_day((a_2.country)::character varying, a_2.cust_no, a_2.zip_code, a_2.addr1)) - 1))::timestamp without time zone
                        END) AND ("substring"((a_2.addr1)::text, 1, 4) <> 'IKEA'::text)))) a_1,
            public.kontakty b_1
          WHERE (((b_1.country_coor)::text =
                CASE
                    WHEN ("substring"((a_1.order_no)::text, 1, 1) = 'Z'::text) THEN ('Z'::character varying)::text
                    ELSE a_1.country
                END) OR ((b_1.country_coor)::text =
                CASE
                    WHEN ((a_1.typ)::text = 'NIE POTWIERDZAĆ'::text) THEN 'Z'::text
                    ELSE NULL::text
                END))
        UNION
         SELECT b_1.mail,
            a_1.cust_id,
            a_1.koor,
            a_1.order_no,
            a_1.line_no,
            a_1.rel_no,
            a_1.descr,
            a_1.country,
            a_1.load_id,
            a_1.ship_date,
            a_1.prom_week,
            a_1.prod_week,
            a_1.prod,
            a_1.planner_buyer,
            a_1.indeks,
            a_1.opis,
            a_1.dop,
            a_1.date_entered,
            a_1.typ,
            a_1.typ_zdarzenia,
            a_1.status_informacji
           FROM ( SELECT a_2.id AS cust_id,
                    a_2.koor,
                    a_2.order_no,
                    a_2.line_no,
                    a_2.rel_no,
                    a_2.descr,
                    a_2.country,
                    a_2.load_id,
                    a_2.ship_date,
                    (a_2.prom_week)::integer AS prom_week,
                    COALESCE(b_2.prod_week, (to_char(
                        CASE
                            WHEN ("substring"((a_2.order_no)::text, 1, 1) = 'Z'::text) THEN (to_date((a_2.prom_week)::text, 'iyyyiw'::text) + '6 days'::interval)
                            ELSE (((to_date((a_2.prom_week)::text, 'iyyyiw'::text) + public.shipment_day((a_2.country)::character varying, a_2.cust_no, a_2.zip_code, a_2.addr1)) - 1))::timestamp without time zone
                        END, 'iyyyiw'::text))::integer) AS prod_week,
                    COALESCE(b_2.prod, a_2.data_dop) AS prod,
                    COALESCE(b_2.planner_buyer, a_2.koor) AS planner_buyer,
                    COALESCE(b_2.indeks,
                        CASE
                            WHEN ((a_2.cust_order_state)::text = ANY (ARRAY[('Zaplanowane'::character varying)::text, ('Zablok. kredyt'::character varying)::text])) THEN a_2.cust_order_state
                            ELSE 'BŁĄD POTW.'::character varying
                        END) AS indeks,
                    COALESCE(b_2.opis, (
                        CASE
                            WHEN ((a_2.cust_order_state)::text = ANY (ARRAY[('Zaplanowane'::character varying)::text, ('Zablok. kredyt'::character varying)::text])) THEN 'Status zamówienia klienta'::text
                            ELSE 'Data realizacji późniejsza od daty obiecanej'::text
                        END)::character varying) AS opis,
                    COALESCE(b_2.dop, a_2.dop_id) AS dop,
                    COALESCE(b_2.date_entered, a_2.date_entered, a_2.objversion) AS date_entered,
                    'NIE POTWIERDZAĆ'::character varying AS typ,
                    b_2.typ_zdarzenia,
                    b_2.status_informacji
                   FROM (( SELECT cust_ord.koor,
                            cust_ord.order_no,
                            cust_ord.line_no,
                            cust_ord.rel_no,
                            cust_ord.line_item_no,
                            cust_ord.last_mail_conf,
                            cust_ord.state_conf,
                            cust_ord.line_state,
                            cust_ord.cust_order_state,
                            ((cust_ord.country)::text ||
                                CASE
                                    WHEN (public.is_interncl(cust_ord.id) = true) THEN '_SITS'::text
                                    ELSE ''::text
                                END) AS country,
                            cust_ord.cust_no,
                            cust_ord.zip_code,
                            cust_ord.addr1,
                            cust_ord.prom_date,
                            cust_ord.prom_week,
                            cust_ord.load_id,
                            cust_ord.ship_date,
                            cust_ord.part_no,
                            cust_ord.descr,
                            cust_ord.configuration,
                            cust_ord.buy_qty_due,
                            cust_ord.desired_qty,
                            cust_ord.qty_invoiced,
                            cust_ord.qty_shipped,
                            cust_ord.qty_assigned,
                            cust_ord.dop_connection_db,
                            cust_ord.dop_id,
                            cust_ord.dop_state,
                            cust_ord.data_dop,
                            cust_ord.dop_qty,
                            cust_ord.dop_made,
                            cust_ord.date_entered,
                            cust_ord.chksum,
                            cust_ord.custid,
                            cust_ord.id,
                            cust_ord.zest,
                            cust_ord.seria0,
                            cust_ord.data0,
                            cust_ord.objversion
                           FROM public.cust_ord
                          WHERE ((cust_ord.order_no)::text IN ( SELECT a_3.order_no
                                   FROM ( SELECT a_1_1.koor,
    a_1_1.order_no,
    a_1_1.line_no,
    a_1_1.rel_no,
    a_1_1.line_item_no,
    a_1_1.last_mail_conf,
    a_1_1.state_conf,
    a_1_1.line_state,
    a_1_1.cust_order_state,
    a_1_1.country,
    a_1_1.cust_no,
    a_1_1.zip_code,
    a_1_1.addr1,
    a_1_1.prom_date,
    a_1_1.prom_week,
    a_1_1.load_id,
    a_1_1.ship_date,
    a_1_1.part_no,
    a_1_1.descr,
    a_1_1.configuration,
    a_1_1.buy_qty_due,
    a_1_1.desired_qty,
    a_1_1.qty_invoiced,
    a_1_1.qty_shipped,
    a_1_1.qty_assigned,
    a_1_1.dop_connection_db,
    a_1_1.dop_id,
    a_1_1.dop_state,
    a_1_1.data_dop,
    a_1_1.dop_qty,
    a_1_1.dop_made,
    a_1_1.date_entered,
    a_1_1.chksum,
    a_1_1.custid,
    a_1_1.id,
    a_1_1.zest,
    a_1_1.seria0,
    a_1_1.data0,
    a_1_1.objversion,
    public.get_refer(a_1_1.addr1) AS reference,
  CASE
   WHEN (((a_1_1.dop_connection_db)::text = 'AUT'::text) AND (a_1_1.dop_state IS NULL)) THEN 1
   ELSE 0
  END AS gotowe
   FROM (public.cust_ord a_1_1
     LEFT JOIN ( SELECT cust_ord_1.order_no
     FROM public.cust_ord cust_ord_1
    WHERE (((cust_ord_1.state_conf)::text = 'Wydrukow.'::text) AND (cust_ord_1.last_mail_conf IS NOT NULL))
    GROUP BY cust_ord_1.order_no) c ON (((c.order_no)::text = (a_1_1.order_no)::text)))
  WHERE ((((a_1_1.state_conf)::text = 'Nie wydruk.'::text) OR (a_1_1.last_mail_conf IS NULL)) AND (public.is_refer(a_1_1.addr1) = true) AND ("substring"((a_1_1.order_no)::text, 1, 1) = 'S'::text) AND ((a_1_1.cust_order_state)::text <> ALL (ARRAY[('Wydane'::character varying)::text, ('Częściowo dostarczone'::character varying)::text, ('Zablok. kredyt'::character varying)::text, ('Zaplanowane'::character varying)::text])) AND ("substring"((a_1_1.part_no)::text, 1, 3) <> ALL (ARRAY['633'::text, '628'::text, '1K1'::text, '1U2'::text, '632'::text])) AND (((c.order_no IS NOT NULL) AND ((a_1_1.dop_connection_db)::text <> 'MAN'::text)) OR (c.order_no IS NULL)))) a_3
                                  GROUP BY a_3.order_no, a_3.cust_no, a_3.reference, a_3.addr1, a_3.country
                                 HAVING ((sum(a_3.gotowe) = 0) AND (max(a_3.objversion) < (now() - '02:00:00'::interval)))))) a_2
                     LEFT JOIN ( SELECT mail.ordid,
                            mail.dop,
                            mail.koor,
                            mail.order_no,
                            mail.line_no,
                            mail.rel_no,
                            mail.part_no,
                            mail.descr,
                            ((mail.country)::text ||
                                CASE
                                    WHEN (public.is_interncl(mail.cust_id) = true) THEN '_SITS'::text
                                    ELSE ''::text
                                END) AS country,
                            mail.prom_date,
                            mail.prom_week,
                            mail.load_id,
                            mail.ship_date,
                            mail.date_entered,
                            mail.cust_id,
                            mail.prod,
                            mail.prod_week,
                            mail.planner_buyer,
                            mail.indeks,
                            mail.opis,
                            mail.typ_zdarzenia,
                            mail.status_informacji,
                            mail.zest,
                            mail.info_handlo,
                            mail.logistyka,
                            mail.seria0,
                            mail.data0,
                            mail.cust_line_stat,
                            mail.ord_objver
                           FROM public.mail
                          WHERE ((mail.info_handlo = true) OR (mail.logistyka = true))) b_2 ON ((b_2.cust_id = a_2.id)))
                  WHERE ((b_2.cust_id IS NOT NULL) OR (((a_2.cust_order_state)::text <> ALL (ARRAY[('Aktywowane'::character varying)::text, ('Częściowo dostarczone'::character varying)::text, ('Zarezerwowane'::character varying)::text])) AND ((a_2.line_state)::text = 'Aktywowana'::text) AND (upper("substring"((a_2.cust_no)::text, 1, 4)) <> 'IKEA'::text) AND ("substring"((a_2.addr1)::text, 1, 4) <> 'IKEA'::text)) OR ((a_2.data_dop >
                        CASE
                            WHEN ("substring"((a_2.order_no)::text, 1, 1) = 'Z'::text) THEN (to_date((a_2.prom_week)::text, 'iyyyiw'::text) + '6 days'::interval)
                            ELSE (((to_date((a_2.prom_week)::text, 'iyyyiw'::text) + public.shipment_day((a_2.country)::character varying, a_2.cust_no, a_2.zip_code, a_2.addr1)) - 1))::timestamp without time zone
                        END) AND ("substring"((a_2.addr1)::text, 1, 4) <> 'IKEA'::text)))) a_1,
            public.kontakty b_1
          WHERE ((b_1.ifs_user)::text = (a_1.koor)::text)) a
     LEFT JOIN public.info_inpotw b ON ((b.cust_id = a.cust_id)))
  ORDER BY a.order_no, a.mail;


ALTER TABLE public.fill_sendmail OWNER TO postgres;

--
-- TOC entry 262 (class 1259 OID 27970)
-- Name: formatka; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.formatka AS
 SELECT a.part_no,
    a.contract,
    b.opis,
    b.mag,
    a.work_day,
    a.type AS rodzaj,
    a.koor,
    a.bal_stock AS bil_mag,
    d.tp AS rodz_prod,
    c.il AS "zagrożenie_prod"
   FROM ((( SELECT a_1.part_no,
            a_1.contract,
            a_1.work_day,
            a_1.type,
            a_1.koor,
            (sum(a_1.purch_qty) - sum(a_1.qty_demand)) AS balance,
            a_1.bal_stock
           FROM ( SELECT demands.part_no,
                    demands.contract,
                        CASE
                            WHEN (demands.work_day < ('now'::text)::date) THEN ('now'::text)::date
                            ELSE demands.work_day
                        END AS work_day,
                    demands.type,
                    demands.koor,
                    demands.purch_qty,
                    demands.qty_demand,
                    demands.balance,
                    demands.bal_stock
                   FROM public.demands
                  ORDER BY
                        CASE
                            WHEN (demands.work_day < ('now'::text)::date) THEN ('now'::text)::date
                            ELSE demands.work_day
                        END) a_1
          GROUP BY a_1.part_no, a_1.contract, a_1.work_day, a_1.type, a_1.koor, a_1.bal_stock) a
     LEFT JOIN ( SELECT a_1.date_required,
            a_1.part_no,
            a_1.contract,
            sum(a_1.prod_qty) AS il
           FROM ( SELECT
                        CASE
                            WHEN (ord_lack.date_required < ('now'::text)::date) THEN ('now'::text)::date
                            ELSE ord_lack.date_required
                        END AS date_required,
                    ord_lack.int_ord,
                    ord_lack.part_no,
                    ord_lack.contract,
                    ord_lack.prod_qty
                   FROM public.ord_lack
                  WHERE ((ord_lack.order_supp_dmd)::text <> 'Zam. zakupu'::text)
                  GROUP BY
                        CASE
                            WHEN (ord_lack.date_required < ('now'::text)::date) THEN ('now'::text)::date
                            ELSE ord_lack.date_required
                        END, ord_lack.int_ord, ord_lack.part_no, ord_lack.contract, ord_lack.prod_qty) a_1
          GROUP BY a_1.date_required, a_1.part_no, a_1.contract) c ON ((((c.part_no)::text = (a.part_no)::text) AND ((c.contract)::text = (a.contract)::text) AND (c.date_required = a.work_day))))
     LEFT JOIN ( SELECT a_1.date_required,
            a_1.part_no,
            a_1.contract,
            string_agg(a_1.rodz_prod, ';'::text ORDER BY a_1.rodz_prod) AS tp
           FROM ( SELECT
                        CASE
                            WHEN (ord_lack.date_required < ('now'::text)::date) THEN ('now'::text)::date
                            ELSE ord_lack.date_required
                        END AS date_required,
                    ord_lack.part_no,
                    ord_lack.contract,
                        CASE
                            WHEN ((ord_lack.order_supp_dmd)::text = 'Zamów. klienta'::text) THEN 'Zam'::text
                            ELSE
                            CASE
                                WHEN (ord_lack.dop <> 0) THEN 'DOP'::text
                                ELSE 'MRP'::text
                            END
                        END AS rodz_prod
                   FROM public.ord_lack
                  WHERE ((ord_lack.order_supp_dmd)::text <> 'Zam. zakupu'::text)
                  GROUP BY
                        CASE
                            WHEN (ord_lack.date_required < ('now'::text)::date) THEN ('now'::text)::date
                            ELSE ord_lack.date_required
                        END, ord_lack.part_no, ord_lack.contract,
                        CASE
                            WHEN ((ord_lack.order_supp_dmd)::text = 'Zamów. klienta'::text) THEN 'Zam'::text
                            ELSE
                            CASE
                                WHEN (ord_lack.dop <> 0) THEN 'DOP'::text
                                ELSE 'MRP'::text
                            END
                        END) a_1
          GROUP BY a_1.date_required, a_1.part_no, a_1.contract) d ON ((((d.part_no)::text = (a.part_no)::text) AND ((d.contract)::text = (a.contract)::text) AND (d.date_required = a.work_day)))),
    public.mag b
  WHERE (((a.koor)::text <> 'LUCPRZ'::text) AND ((b.indeks)::text = (a.part_no)::text) AND ((b.contract)::text = (a.contract)::text) AND (a.bal_stock < (0)::double precision))
  WITH NO DATA;


ALTER TABLE public.formatka OWNER TO postgres;

--
-- TOC entry 249 (class 1259 OID 17417)
-- Name: ord_lack_bil; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ord_lack_bil (
    dop integer NOT NULL,
    dop_lin integer,
    data_dop date,
    day_shift integer,
    order_no character varying(25),
    line_no character varying(5),
    rel_no character varying(5),
    int_ord integer,
    contract character varying(10),
    order_supp_dmd character varying(25),
    wrkc character varying(15),
    next_wrkc character varying(15),
    part_no character varying(15),
    descr character varying(150),
    part_code character varying(20),
    date_required date,
    ord_state character varying(20),
    ord_date date,
    prod_qty double precision,
    qty_supply double precision,
    qty_demand double precision,
    dat_creat date,
    chksum integer,
    id uuid NOT NULL
);


ALTER TABLE public.ord_lack_bil OWNER TO postgres;

--
-- TOC entry 264 (class 1259 OID 28743)
-- Name: formatka_bil; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.formatka_bil AS
 SELECT a.part_no,
    a.contract,
    b.opis,
    b.mag,
    a.work_day,
    a.type AS rodzaj,
    a.koor,
    a.balance AS bilans,
    d.tp AS rodz_prod,
    c.il AS "zagrożenie_prod"
   FROM ((( SELECT a_1.part_no,
            a_1.contract,
            a_1.work_day,
            a_1.type,
            a_1.koor,
            min(a_1.balance) AS balance,
            (sum(a_1.qty_demand) * ('-1'::integer)::double precision) AS bal_stock
           FROM ( SELECT demands.part_no,
                    demands.contract,
                        CASE
                            WHEN (demands.work_day < ('now'::text)::date) THEN ('now'::text)::date
                            ELSE demands.work_day
                        END AS work_day,
                    demands.type,
                    demands.koor,
                    demands.purch_qty,
                    demands.qty_demand,
                        CASE
                            WHEN ((demands.balance * ('-1'::integer)::double precision) > demands.qty_demand) THEN (demands.qty_demand * ('-1'::integer)::double precision)
                            ELSE demands.balance
                        END AS balance,
                    demands.bal_stock
                   FROM public.demands
                  ORDER BY
                        CASE
                            WHEN (demands.work_day < ('now'::text)::date) THEN ('now'::text)::date
                            ELSE demands.work_day
                        END) a_1
          GROUP BY a_1.part_no, a_1.contract, a_1.work_day, a_1.type, a_1.koor) a
     LEFT JOIN ( SELECT a_1.date_required,
            a_1.part_no,
            a_1.contract,
            sum(a_1.prod_qty) AS il
           FROM ( SELECT
                        CASE
                            WHEN (ord_lack_bil.date_required < ('now'::text)::date) THEN ('now'::text)::date
                            ELSE ord_lack_bil.date_required
                        END AS date_required,
                    ord_lack_bil.int_ord,
                    ord_lack_bil.part_no,
                    ord_lack_bil.contract,
                    ord_lack_bil.prod_qty
                   FROM public.ord_lack_bil
                  WHERE ((ord_lack_bil.order_supp_dmd)::text <> 'Zam. zakupu'::text)
                  GROUP BY
                        CASE
                            WHEN (ord_lack_bil.date_required < ('now'::text)::date) THEN ('now'::text)::date
                            ELSE ord_lack_bil.date_required
                        END, ord_lack_bil.int_ord, ord_lack_bil.part_no, ord_lack_bil.contract, ord_lack_bil.prod_qty) a_1
          GROUP BY a_1.date_required, a_1.part_no, a_1.contract) c ON ((((c.part_no)::text = (a.part_no)::text) AND ((c.contract)::text = (a.contract)::text) AND (c.date_required = a.work_day))))
     LEFT JOIN ( SELECT a_1.date_required,
            a_1.part_no,
            a_1.contract,
            string_agg(a_1.rodz_prod, ';'::text ORDER BY a_1.rodz_prod) AS tp
           FROM ( SELECT
                        CASE
                            WHEN (ord_lack_bil.date_required < ('now'::text)::date) THEN ('now'::text)::date
                            ELSE ord_lack_bil.date_required
                        END AS date_required,
                    ord_lack_bil.part_no,
                    ord_lack_bil.contract,
                        CASE
                            WHEN ((ord_lack_bil.order_supp_dmd)::text = 'Zamów. klienta'::text) THEN 'Zam'::text
                            ELSE
                            CASE
                                WHEN (ord_lack_bil.dop <> 0) THEN 'DOP'::text
                                ELSE 'MRP'::text
                            END
                        END AS rodz_prod
                   FROM public.ord_lack_bil
                  WHERE ((ord_lack_bil.order_supp_dmd)::text <> 'Zam. zakupu'::text)
                  GROUP BY
                        CASE
                            WHEN (ord_lack_bil.date_required < ('now'::text)::date) THEN ('now'::text)::date
                            ELSE ord_lack_bil.date_required
                        END, ord_lack_bil.part_no, ord_lack_bil.contract,
                        CASE
                            WHEN ((ord_lack_bil.order_supp_dmd)::text = 'Zamów. klienta'::text) THEN 'Zam'::text
                            ELSE
                            CASE
                                WHEN (ord_lack_bil.dop <> 0) THEN 'DOP'::text
                                ELSE 'MRP'::text
                            END
                        END) a_1
          GROUP BY a_1.date_required, a_1.part_no, a_1.contract) d ON ((((d.part_no)::text = (a.part_no)::text) AND ((d.contract)::text = (a.contract)::text) AND (d.date_required = a.work_day)))),
    public.mag b
  WHERE (((a.koor)::text <> 'LUCPRZ'::text) AND ((b.indeks)::text = (a.part_no)::text) AND ((b.contract)::text = (a.contract)::text) AND (a.balance < (0)::double precision))
  WITH NO DATA;


ALTER TABLE public.formatka_bil OWNER TO postgres;

--
-- TOC entry 250 (class 1259 OID 17427)
-- Name: lack_ord_demands; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.lack_ord_demands (
    type character varying(10),
    ord_dop character varying(30),
    line integer,
    part_no character varying(30),
    date_required date,
    qty_required double precision,
    state character varying(25),
    id uuid NOT NULL
);


ALTER TABLE public.lack_ord_demands OWNER TO postgres;

--
-- TOC entry 251 (class 1259 OID 17430)
-- Name: late_ord; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.late_ord (
    ordid text NOT NULL,
    dop integer,
    koor character varying(15),
    order_no character varying(15),
    line_no character varying(5),
    rel_no character varying(5),
    part_no character varying(25),
    descr character varying(150),
    country character varying(3),
    prom_date date,
    prom_week integer,
    load_id bigint,
    ship_date date,
    date_entered timestamp without time zone,
    cust_id uuid NOT NULL,
    prod date,
    prod_week integer,
    planner_buyer character varying(500),
    indeks character varying(500),
    opis character varying(1000),
    typ_zdarzenia character varying(1000),
    status_informacji character varying(500),
    zest character varying(50),
    info_handlo boolean,
    logistyka boolean,
    seria0 boolean,
    data0 date,
    cust_line_stat character varying(50),
    ord_objver timestamp without time zone,
    data_dop date
);


ALTER TABLE public.late_ord OWNER TO postgres;

--
-- TOC entry 252 (class 1259 OID 17435)
-- Name: ord_demands; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ord_demands (
    dop integer NOT NULL,
    dop_lin integer,
    data_dop date,
    day_shift integer,
    order_no character varying(25),
    line_no character varying(5),
    rel_no character varying(5),
    int_ord integer,
    contract character varying(10),
    order_supp_dmd character varying(25),
    wrkc character varying(15),
    next_wrkc character varying(15),
    part_no character varying(15) NOT NULL,
    descr character varying(150),
    part_code character varying(20),
    date_required date,
    ord_state character varying(20),
    ord_date date,
    prod_qty double precision,
    qty_supply double precision,
    qty_demand double precision,
    dat_creat date,
    chksum integer,
    id uuid NOT NULL
);


ALTER TABLE public.ord_demands OWNER TO postgres;

--
-- TOC entry 253 (class 1259 OID 17438)
-- Name: serv_idle; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.serv_idle (
    typ character varying(50),
    start_idle timestamp without time zone,
    stop_idle timestamp without time zone
);


ALTER TABLE public.serv_idle OWNER TO postgres;

--
-- TOC entry 254 (class 1259 OID 17441)
-- Name: server_query; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.server_query (
    start_date timestamp without time zone,
    end_dat timestamp without time zone,
    errors_found integer,
    id uuid NOT NULL,
    log text
);


ALTER TABLE public.server_query OWNER TO postgres;

--
-- TOC entry 255 (class 1259 OID 17446)
-- Name: shop_ord; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.shop_ord (
    dop integer,
    int_ord integer,
    ord_id bigint,
    order_no character varying(50),
    objstate character varying(12),
    part_no character varying(25),
    configuration_id character varying(25),
    work_center character varying(15),
    work_day date,
    start_hour timestamp without time zone,
    hours double precision,
    qty double precision,
    id uuid NOT NULL
);


ALTER TABLE public.shop_ord OWNER TO postgres;

--
-- TOC entry 256 (class 1259 OID 17449)
-- Name: to_mail; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.to_mail AS
 SELECT a.ordid,
    a.dop,
    a.koor,
    a.order_no,
    a.line_no,
    a.rel_no,
    a.part_no,
    a.descr,
    a.country,
    a.prom_date,
    a.prom_week,
    a.load_id,
    a.ship_date,
    a.date_entered,
    a.cust_id,
    a.prod,
    a.prod_week,
    a.planner_buyer,
    a.indeks,
    a.opis,
    a.typ_zdarzenia,
        CASE
            WHEN ((a.status_informacji <> 'WYKONANIE'::text) AND public.is_seria0(a.cust_id) AND (a.prod <= public.date_ser0(a.cust_id))) THEN 'WYKONANIE'::text
            ELSE a.status_informacji
        END AS status_informacji,
    a.zest,
        CASE
            WHEN (a.prom_week < a.prod_week) THEN true
            ELSE false
        END AS info_handlo,
        CASE
            WHEN (a.ship_date IS NOT NULL) THEN
            CASE
                WHEN (a.ship_date <= a.prod) THEN true
                ELSE false
            END
            ELSE false
        END AS logistyka,
    public.is_seria0(a.cust_id) AS seria0,
    public.date_ser0(a.cust_id) AS data0,
    public.cust_lin_stat(a.cust_id) AS cust_line_stat,
    public.ord_objver(a.cust_id) AS ord_objver
   FROM ( SELECT c.ordid,
            c.dop,
            c.koor,
            c.order_no,
            c.line_no,
            c.rel_no,
            c.part_no,
            c.descr,
            c.country,
            c.prom_date,
            c.prom_week,
            c.load_id,
            c.ship_date,
            c.date_entered,
            c.cust_id,
            a_1.prod,
            (
                CASE
                    WHEN (date_part('isodow'::text, a_1.prod) < (a_1.shipment_day)::double precision) THEN to_char((a_1.prod)::timestamp with time zone, 'IYYYIW'::text)
                    ELSE to_char((a_1.prod + '7 days'::interval), 'IYYYIW'::text)
                END)::integer AS prod_week,
            b.planner_buyer,
            b.indeks,
            b.opis,
            b.typ_zdarzenia,
            b.status_informacji,
            b.zest
           FROM ((( SELECT b_1.ordid,
                    a_2.ordf,
                    a_2.shipment_day,
                    a_2.prod
                   FROM ( SELECT COALESCE(braki.zest, (braki.ordid)::character varying) AS ordf,
                            braki.shipment_day,
                            max(braki.prod_date) AS prod
                           FROM public.braki
                          WHERE (substr(braki.ordid, 1, 1) <> 'O'::text)
                          GROUP BY COALESCE(braki.zest, (braki.ordid)::character varying), braki.shipment_day) a_2,
                    ( SELECT braki.ordid,
                            braki.zest
                           FROM public.braki
                          WHERE (substr(braki.ordid, 1, 1) <> 'O'::text)
                          GROUP BY braki.ordid, braki.zest) b_1
                  WHERE ((COALESCE(b_1.zest, (b_1.ordid)::character varying))::text = (a_2.ordf)::text)) a_1
             LEFT JOIN ( SELECT COALESCE(a_2.zest, a_2.ordid) AS ordf,
                    a_2.prod_date,
                    string_agg((a_2.planner_buyer)::text, (';'::text || chr(13)) ORDER BY a_2.indeks) AS planner_buyer,
                    string_agg((a_2.indeks)::text, (';'::text || chr(13)) ORDER BY (a_2.indeks)::text) AS indeks,
                    string_agg((a_2.opis)::text, (';'::text || chr(13)) ORDER BY a_2.indeks) AS opis,
                    string_agg((a_2.typ_zdarzenia)::text, (';'::text || chr(13)) ORDER BY a_2.indeks) AS typ_zdarzenia,
                    string_agg((a_2.status_informacji)::text, ';'::text ORDER BY a_2.indeks) AS status_informacji,
                    a_2.zest
                   FROM ( SELECT COALESCE(braki.zest, (braki.ordid)::character varying) AS ordid,
                            braki.prod_date,
                            braki.planner_buyer,
                            braki.indeks,
                            braki.opis,
                            braki.typ_zdarzenia,
                            braki.status_informacji,
                            braki.zest
                           FROM public.braki
                          GROUP BY COALESCE(braki.zest, (braki.ordid)::character varying), braki.prod_date, braki.planner_buyer, braki.indeks, braki.opis, braki.typ_zdarzenia, braki.status_informacji, braki.zest
                          ORDER BY braki.prod_date, braki.indeks) a_2
                  GROUP BY COALESCE(a_2.zest, a_2.ordid), a_2.prod_date, a_2.zest) b ON ((((b.ordf)::text = (a_1.ordf)::text) AND (b.prod_date = a_1.prod))))
             LEFT JOIN ( SELECT braki.ordid,
                    braki.dop,
                    braki.koor,
                    braki.order_no,
                    braki.line_no,
                    braki.rel_no,
                    braki.part_no,
                    braki.descr,
                    braki.country,
                    braki.prom_date,
                    (braki.prom_week)::integer AS prom_week,
                    braki.load_id,
                    braki.ship_date,
                    braki.date_entered,
                    braki.cust_id
                   FROM public.braki
                  GROUP BY braki.ordid, braki.dop, braki.koor, braki.order_no, braki.line_no, braki.rel_no, braki.part_no, braki.descr, braki.country, braki.prom_date, (braki.prom_week)::integer, braki.load_id, braki.ship_date, braki.date_entered, braki.cust_id) c ON ((c.ordid = a_1.ordid)))
        UNION ALL
         SELECT
                CASE
                    WHEN (b.dop_id = 0) THEN ('O '::text || (b.order_no)::text)
                    ELSE ('D'::text || to_char(b.dop_id, '9999999999'::text))
                END AS ordid,
            b.dop_id AS dop,
            b.koor,
            b.order_no,
            b.line_no,
            b.rel_no,
            b.part_no,
            b.descr,
            b.country,
            b.prom_date,
            (b.prom_week)::integer AS prom_week,
            b.load_id,
            b.ship_date,
            b.date_entered,
            b.id,
            a_1.prod,
            (
                CASE
                    WHEN (date_part('isodow'::text, a_1.prod) < (a_1.shipment_day)::double precision) THEN to_char((a_1.prod)::timestamp with time zone, 'IYYYIW'::text)
                    ELSE to_char((a_1.prod + '7 days'::interval), 'IYYYIW'::text)
                END)::integer AS prod_week,
            'RADKOS'::text AS planner_buyer,
            (((a_1.zest)::text || ' Brak:'::text) || c.indeks) AS indeks,
            ('Przesunięcie zestawu - brak mat. Wspólna data :'::text || c.opis) AS opis,
            c.typ_zdarzenia,
            c.status_informacji,
            a_1.zest
           FROM ((( SELECT braki.zest,
                    braki.shipment_day,
                    max(braki.prod_date) AS prod
                   FROM public.braki
                  WHERE ((substr(braki.ordid, 1, 1) <> 'O'::text) AND (braki.zest IS NOT NULL))
                  GROUP BY braki.zest, braki.shipment_day) a_1
             LEFT JOIN ( SELECT "Mieszanie z Konfiguracjami".ordid,
                    "Mieszanie z Konfiguracjami".dop,
                    "Mieszanie z Konfiguracjami".koor,
                    "Mieszanie z Konfiguracjami".order_no,
                    "Mieszanie z Konfiguracjami".line_no,
                    "Mieszanie z Konfiguracjami".rel_no,
                    "Mieszanie z Konfiguracjami".part_no,
                    "Mieszanie z Konfiguracjami".descr,
                    "Mieszanie z Konfiguracjami".country,
                    "Mieszanie z Konfiguracjami".prom_date,
                    "Mieszanie z Konfiguracjami".prom_week,
                    "Mieszanie z Konfiguracjami".load_id,
                    "Mieszanie z Konfiguracjami".ship_date,
                    "Mieszanie z Konfiguracjami".date_entered,
                    "Mieszanie z Konfiguracjami".id,
                    "Mieszanie z Konfiguracjami".prod,
                    "Mieszanie z Konfiguracjami".prod_week,
                    "Mieszanie z Konfiguracjami".planner_buyer,
                    "Mieszanie z Konfiguracjami".indeks,
                    "Mieszanie z Konfiguracjami".opis,
                    "Mieszanie z Konfiguracjami".typ_zdarzenia,
                    "Mieszanie z Konfiguracjami".status_informacji,
                    "Mieszanie z Konfiguracjami".zest
                   FROM public."Mieszanie z Konfiguracjami") f ON (((f.zest)::text = (a_1.zest)::text)))
             LEFT JOIN ( SELECT a_2.prod_date,
                    string_agg((a_2.planner_buyer)::text, (';'::text || chr(13)) ORDER BY a_2.indeks) AS planner_buyer,
                    string_agg((a_2.indeks)::text, (';'::text || chr(13)) ORDER BY (a_2.indeks)::text) AS indeks,
                    string_agg((a_2.opis)::text, (';'::text || chr(13)) ORDER BY a_2.indeks) AS opis,
                    string_agg((a_2.typ_zdarzenia)::text, (';'::text || chr(13)) ORDER BY a_2.indeks) AS typ_zdarzenia,
                    string_agg((a_2.status_informacji)::text, ';'::text ORDER BY a_2.indeks) AS status_informacji,
                    a_2.zest
                   FROM ( SELECT braki.prod_date,
                            braki.planner_buyer,
                            braki.indeks,
                            braki.opis,
                            braki.typ_zdarzenia,
                            braki.status_informacji,
                            braki.zest
                           FROM public.braki
                          GROUP BY braki.prod_date, braki.planner_buyer, braki.indeks, braki.opis, braki.typ_zdarzenia, braki.status_informacji, braki.zest
                          ORDER BY braki.prod_date, braki.indeks) a_2
                  GROUP BY a_2.prod_date, a_2.zest) c ON ((((c.zest)::text = (a_1.zest)::text) AND (c.prod_date = a_1.prod)))),
            (public.cust_ord b
             LEFT JOIN public.braki d ON ((d.dop = b.dop_id)))
          WHERE (((b.zest)::text = (a_1.zest)::text) AND (d.ordid IS NULL) AND (f.zest IS NULL))
        UNION ALL
         SELECT
                CASE
                    WHEN (b.dop_id = 0) THEN ('O '::text || (b.order_no)::text)
                    ELSE ('D'::text || to_char(b.dop_id, '9999999999'::text))
                END AS ordid,
            b.dop_id AS dop,
            b.koor,
            b.order_no,
            b.line_no,
            b.rel_no,
            b.part_no,
            b.descr,
            b.country,
            b.prom_date,
            (b.prom_week)::integer AS prom_week,
            b.load_id,
            b.ship_date,
            b.date_entered,
            b.id,
            b.prod,
            (
                CASE
                    WHEN (date_part('isodow'::text, b.prod) < (b.shipment_day)::double precision) THEN to_char((b.prod)::timestamp with time zone, 'IYYYIW'::text)
                    ELSE to_char((b.prod + '7 days'::interval), 'IYYYIW'::text)
                END)::integer AS prod_week,
            'RADKOS'::text AS planner_buyer,
            'Seria Zero '::text AS indeks,
                CASE
                    WHEN (b.seria0 = true) THEN 'Przesunięcie zgodnie z IFS'::text
                    ELSE 'Wykonanie 3tyg po serii 0'::text
                END AS opis,
            'Wewn. procedura'::text AS typ_zdarzenia,
            'WYKONANIE'::text AS status_informacji,
            b.zest
           FROM ((( SELECT b_1.koor,
                    b_1.order_no,
                    b_1.line_no,
                    b_1.rel_no,
                    b_1.line_item_no,
                    b_1.last_mail_conf,
                    b_1.state_conf,
                    b_1.line_state,
                    b_1.cust_order_state,
                    b_1.country,
                    b_1.cust_no,
                    b_1.zip_code,
                    b_1.addr1,
                    b_1.prom_date,
                    b_1.prom_week,
                    b_1.load_id,
                    b_1.ship_date,
                    b_1.part_no,
                    b_1.descr,
                    b_1.configuration,
                    b_1.buy_qty_due,
                    b_1.desired_qty,
                    b_1.qty_invoiced,
                    b_1.qty_shipped,
                    b_1.qty_assigned,
                    b_1.dop_connection_db,
                    b_1.dop_id,
                    b_1.dop_state,
                    b_1.data_dop,
                    b_1.dop_qty,
                    b_1.dop_made,
                    b_1.date_entered,
                    b_1.chksum,
                    b_1.custid,
                    b_1.id,
                    b_1.zest,
                    b_1.seria0,
                    b_1.data0,
                    b_1.objversion,
                        CASE
                            WHEN (b_1.seria0 = true) THEN public.wrk_day(public.wrk_near_count(b_1.data0))
                            ELSE public.wrk_day(public.wrk_near_count(((b_1.data0 + '21 days'::interval))::date))
                        END AS prod,
                    public.shipment_day(b_1.country, b_1.cust_no, b_1.zip_code, b_1.addr1) AS shipment_day
                   FROM ( SELECT b_1_1.koor,
                            b_1_1.order_no,
                            b_1_1.line_no,
                            b_1_1.rel_no,
                            b_1_1.line_item_no,
                            b_1_1.last_mail_conf,
                            b_1_1.state_conf,
                            b_1_1.line_state,
                            b_1_1.cust_order_state,
                            b_1_1.country,
                            b_1_1.cust_no,
                            b_1_1.zip_code,
                            b_1_1.addr1,
                            b_1_1.prom_date,
                            b_1_1.prom_week,
                            b_1_1.load_id,
                            b_1_1.ship_date,
                            b_1_1.part_no,
                            b_1_1.descr,
                            b_1_1.configuration,
                            b_1_1.buy_qty_due,
                            b_1_1.desired_qty,
                            b_1_1.qty_invoiced,
                            b_1_1.qty_shipped,
                            b_1_1.qty_assigned,
                            b_1_1.dop_connection_db,
                            b_1_1.dop_id,
                            b_1_1.dop_state,
                            b_1_1.data_dop,
                            b_1_1.dop_qty,
                            b_1_1.dop_made,
                            b_1_1.date_entered,
                            b_1_1.chksum,
                            b_1_1.custid,
                            b_1_1.id,
                            b_1_1.zest,
                            b_1_1.seria0,
                            b_1_1.data0,
                            b_1_1.objversion
                           FROM (public.cust_ord b_1_1
                             LEFT JOIN ( SELECT "Mieszanie z Konfiguracjami".ordid,
                                    "Mieszanie z Konfiguracjami".dop,
                                    "Mieszanie z Konfiguracjami".koor,
                                    "Mieszanie z Konfiguracjami".order_no,
                                    "Mieszanie z Konfiguracjami".line_no,
                                    "Mieszanie z Konfiguracjami".rel_no,
                                    "Mieszanie z Konfiguracjami".part_no,
                                    "Mieszanie z Konfiguracjami".descr,
                                    "Mieszanie z Konfiguracjami".country,
                                    "Mieszanie z Konfiguracjami".prom_date,
                                    "Mieszanie z Konfiguracjami".prom_week,
                                    "Mieszanie z Konfiguracjami".load_id,
                                    "Mieszanie z Konfiguracjami".ship_date,
                                    "Mieszanie z Konfiguracjami".date_entered,
                                    "Mieszanie z Konfiguracjami".id,
                                    "Mieszanie z Konfiguracjami".prod,
                                    "Mieszanie z Konfiguracjami".prod_week,
                                    "Mieszanie z Konfiguracjami".planner_buyer,
                                    "Mieszanie z Konfiguracjami".indeks,
                                    "Mieszanie z Konfiguracjami".opis,
                                    "Mieszanie z Konfiguracjami".typ_zdarzenia,
                                    "Mieszanie z Konfiguracjami".status_informacji,
                                    "Mieszanie z Konfiguracjami".zest
                                   FROM public."Mieszanie z Konfiguracjami") z ON ((z.dop = b_1_1.dop_id)))
                          WHERE ((((b_1_1.seria0 = true) AND (b_1_1.data_dop <> public.wrk_day(public.wrk_near_count(b_1_1.data0)))) OR ((b_1_1.data0 IS NOT NULL) AND (b_1_1.seria0 = false) AND (public.wrk_day(public.wrk_near_count(((b_1_1.data0 + '21 days'::interval))::date)) > b_1_1.data_dop))) AND ((b_1_1.line_state)::text = 'Aktywowana'::text) AND (z.dop IS NULL))
                        UNION ALL
                         SELECT a_2.koor,
                            a_2.order_no,
                            a_2.line_no,
                            a_2.rel_no,
                            a_2.line_item_no,
                            a_2.last_mail_conf,
                            a_2.state_conf,
                            a_2.line_state,
                            a_2.cust_order_state,
                            a_2.country,
                            a_2.cust_no,
                            a_2.zip_code,
                            a_2.addr1,
                            a_2.prom_date,
                            a_2.prom_week,
                            a_2.load_id,
                            a_2.ship_date,
                            a_2.part_no,
                            a_2.descr,
                            a_2.configuration,
                            a_2.buy_qty_due,
                            a_2.desired_qty,
                            a_2.qty_invoiced,
                            a_2.qty_shipped,
                            a_2.qty_assigned,
                            a_2.dop_connection_db,
                            a_2.dop_id,
                            a_2.dop_state,
                            a_2.data_dop,
                            a_2.dop_qty,
                            a_2.dop_made,
                            a_2.date_entered,
                            a_2.chksum,
                            a_2.custid,
                            a_2.id,
                            a_2.zest,
                            a_2.seria0,
                            b_2.prod AS data0,
                            a_2.objversion
                           FROM public.cust_ord a_2,
                            ( SELECT cust_ord.zest,
                                    max(cust_ord.data0) AS prod
                                   FROM public.cust_ord
                                  WHERE ((cust_ord.zest IS NOT NULL) AND (((cust_ord.seria0 = true) AND (cust_ord.data_dop <> public.wrk_day(public.wrk_near_count(cust_ord.data0)))) OR ((cust_ord.data0 IS NOT NULL) AND (cust_ord.seria0 = false) AND (public.wrk_day(public.wrk_near_count(((cust_ord.data0 + '21 days'::interval))::date)) > cust_ord.data_dop))) AND ((cust_ord.line_state)::text = 'Aktywowana'::text))
                                  GROUP BY cust_ord.zest) b_2
                          WHERE (((b_2.zest)::text = (a_2.zest)::text) AND (a_2.seria0 = false) AND (a_2.data0 IS NULL))) b_1) b
             LEFT JOIN ( SELECT braki.ordid,
                    braki.l_ordid,
                    braki.indeks,
                    braki.opis,
                    braki.planner_buyer,
                    braki.mag,
                    braki.data_dost,
                    braki.date_reuired,
                    braki.wlk_dost,
                    braki.bilans,
                    braki.typ_zdarzenia,
                    braki.status_informacji,
                    braki.dop,
                    braki.dop_lin,
                    braki.data_dop,
                    braki.zlec,
                    braki.prod_date,
                    braki.max_posible_prod,
                    braki.max_prod_date,
                    braki.ord_supp_dmd,
                    braki.part_code,
                    braki.ord_state,
                    braki.prod_qty,
                    braki.qty_supply,
                    braki.qty_demand,
                    braki.koor,
                    braki.order_no,
                    braki.line_no,
                    braki.rel_no,
                    braki.part_no,
                    braki.descr,
                    braki.configuration,
                    braki.last_mail_conf,
                    braki.prom_date,
                    braki.prom_week,
                    braki.load_id,
                    braki.ship_date,
                    braki.state_conf,
                    braki.line_state,
                    braki.cust_ord_state,
                    braki.country,
                    braki.shipment_day,
                    braki.date_entered,
                    braki.sort_ord,
                    braki.zest,
                    braki.ord_assinged,
                    braki.id,
                    braki.cust_id
                   FROM public.braki) a_1 ON (((COALESCE(a_1.zest, (a_1.dop)::character varying))::text = (COALESCE(b.zest, (b.dop_id)::character varying))::text)))
             LEFT JOIN ( SELECT "Mieszanie z Konfiguracjami".ordid,
                    "Mieszanie z Konfiguracjami".dop,
                    "Mieszanie z Konfiguracjami".koor,
                    "Mieszanie z Konfiguracjami".order_no,
                    "Mieszanie z Konfiguracjami".line_no,
                    "Mieszanie z Konfiguracjami".rel_no,
                    "Mieszanie z Konfiguracjami".part_no,
                    "Mieszanie z Konfiguracjami".descr,
                    "Mieszanie z Konfiguracjami".country,
                    "Mieszanie z Konfiguracjami".prom_date,
                    "Mieszanie z Konfiguracjami".prom_week,
                    "Mieszanie z Konfiguracjami".load_id,
                    "Mieszanie z Konfiguracjami".ship_date,
                    "Mieszanie z Konfiguracjami".date_entered,
                    "Mieszanie z Konfiguracjami".id,
                    "Mieszanie z Konfiguracjami".prod,
                    "Mieszanie z Konfiguracjami".prod_week,
                    "Mieszanie z Konfiguracjami".planner_buyer,
                    "Mieszanie z Konfiguracjami".indeks,
                    "Mieszanie z Konfiguracjami".opis,
                    "Mieszanie z Konfiguracjami".typ_zdarzenia,
                    "Mieszanie z Konfiguracjami".status_informacji,
                    "Mieszanie z Konfiguracjami".zest
                   FROM public."Mieszanie z Konfiguracjami") f ON (((f.zest)::text = (b.zest)::text)))
          WHERE ((a_1.ordid IS NULL) AND (f.zest IS NULL))
        UNION ALL
         SELECT ('D'::text || to_char(a_1.dop_id, '9999999999'::text)) AS ordid,
            a_1.dop_id AS dop,
            a_1.koor,
            a_1.order_no,
            a_1.line_no,
            a_1.rel_no,
            a_1.part_no,
            a_1.descr,
            a_1.country,
            a_1.prom_date,
            (a_1.prom_week)::integer AS prom_week,
            a_1.load_id,
            a_1.ship_date,
            a_1.date_entered,
            a_1.id,
            b.dop_dta AS prod,
            (
                CASE
                    WHEN (date_part('isodow'::text, b.dop_dta) < (public.shipment_day(a_1.country, a_1.cust_no, a_1.zip_code, a_1.addr1))::double precision) THEN to_char((b.dop_dta)::timestamp with time zone, 'IYYYIW'::text)
                    ELSE to_char((b.dop_dta + '7 days'::interval), 'IYYYIW'::text)
                END)::integer AS prod_week,
            'RADKOS'::text AS planner_buyer,
            'ZESTAW'::text AS indeks,
            'Ustawienie wspólnej daty'::text AS opis,
            'Wewn. procedura'::text AS typ_zdarzenia,
            'WYKONANIE'::text AS status_informacji,
            a_1.zest
           FROM public.cust_ord a_1,
            (((( SELECT cust_ord.zest,
                    COALESCE(min(
                        CASE
                            WHEN ((cust_ord.dop_state)::text <> 'Unreleased'::text) THEN cust_ord.data_dop
                            ELSE NULL::date
                        END), min(cust_ord.data_dop)) AS dop_dta
                   FROM public.cust_ord
                  WHERE ((cust_ord.zest IS NOT NULL) AND ((cust_ord.cust_order_state)::text <> ALL (ARRAY[('Zablok. kredyt'::character varying)::text, ('Zaplanowane'::character varying)::text])) AND ((cust_ord.line_state)::text = 'Aktywowana'::text) AND ((cust_ord.addr1)::text !~~ '%IKEA%'::text) AND (cust_ord.data_dop > ('now'::text)::date))
                  GROUP BY cust_ord.zest) b
             LEFT JOIN ( SELECT braki.zest
                   FROM public.braki) c ON (((c.zest)::text = (b.zest)::text)))
             LEFT JOIN ( SELECT "Mieszanie z Konfiguracjami".ordid,
                    "Mieszanie z Konfiguracjami".dop,
                    "Mieszanie z Konfiguracjami".koor,
                    "Mieszanie z Konfiguracjami".order_no,
                    "Mieszanie z Konfiguracjami".line_no,
                    "Mieszanie z Konfiguracjami".rel_no,
                    "Mieszanie z Konfiguracjami".part_no,
                    "Mieszanie z Konfiguracjami".descr,
                    "Mieszanie z Konfiguracjami".country,
                    "Mieszanie z Konfiguracjami".prom_date,
                    "Mieszanie z Konfiguracjami".prom_week,
                    "Mieszanie z Konfiguracjami".load_id,
                    "Mieszanie z Konfiguracjami".ship_date,
                    "Mieszanie z Konfiguracjami".date_entered,
                    "Mieszanie z Konfiguracjami".id,
                    "Mieszanie z Konfiguracjami".prod,
                    "Mieszanie z Konfiguracjami".prod_week,
                    "Mieszanie z Konfiguracjami".planner_buyer,
                    "Mieszanie z Konfiguracjami".indeks,
                    "Mieszanie z Konfiguracjami".opis,
                    "Mieszanie z Konfiguracjami".typ_zdarzenia,
                    "Mieszanie z Konfiguracjami".status_informacji,
                    "Mieszanie z Konfiguracjami".zest
                   FROM public."Mieszanie z Konfiguracjami") f ON (((f.zest)::text = (b.zest)::text)))
             LEFT JOIN ( SELECT cust_ord.zest,
                    max(cust_ord.data0) AS prod
                   FROM public.cust_ord
                  WHERE ((cust_ord.zest IS NOT NULL) AND (((cust_ord.seria0 = true) AND (cust_ord.data_dop <> public.wrk_day(public.wrk_near_count(cust_ord.data0)))) OR ((cust_ord.data0 IS NOT NULL) AND (cust_ord.seria0 = false) AND (public.wrk_day(public.wrk_near_count(((cust_ord.data0 + '21 days'::interval))::date)) > cust_ord.data_dop))) AND ((cust_ord.line_state)::text = 'Aktywowana'::text))
                  GROUP BY cust_ord.zest) d ON (((d.zest)::text = (b.zest)::text)))
          WHERE ((c.zest IS NULL) AND (f.zest IS NULL) AND (d.zest IS NULL) AND ((a_1.cust_order_state)::text <> ALL (ARRAY[('Zablok. kredyt'::character varying)::text, ('Zaplanowane'::character varying)::text])) AND ((a_1.line_state)::text = 'Aktywowana'::text) AND ((a_1.addr1)::text !~~ '%IKEA%'::text) AND (a_1.zest IS NOT NULL) AND ((a_1.zest)::text = (b.zest)::text) AND (a_1.data_dop <> b.dop_dta) AND (
                CASE
                    WHEN (a_1.ship_date IS NOT NULL) THEN
                    CASE
                        WHEN (a_1.ship_date <= b.dop_dta) THEN true
                        ELSE false
                    END
                    ELSE false
                END = false) AND (
                CASE
                    WHEN ((a_1.prom_week)::integer < (
                    CASE
                        WHEN (date_part('isodow'::text, b.dop_dta) < (public.shipment_day(a_1.country, a_1.cust_no, a_1.zip_code, a_1.addr1))::double precision) THEN to_char((b.dop_dta)::timestamp with time zone, 'IYYYIW'::text)
                        ELSE to_char((b.dop_dta + '7 days'::interval), 'IYYYIW'::text)
                    END)::integer) THEN true
                    ELSE false
                END = false))
        UNION ALL
         SELECT "Mieszanie z Konfiguracjami".ordid,
            "Mieszanie z Konfiguracjami".dop,
            "Mieszanie z Konfiguracjami".koor,
            "Mieszanie z Konfiguracjami".order_no,
            "Mieszanie z Konfiguracjami".line_no,
            "Mieszanie z Konfiguracjami".rel_no,
            "Mieszanie z Konfiguracjami".part_no,
            "Mieszanie z Konfiguracjami".descr,
            "Mieszanie z Konfiguracjami".country,
            "Mieszanie z Konfiguracjami".prom_date,
            "Mieszanie z Konfiguracjami".prom_week,
            "Mieszanie z Konfiguracjami".load_id,
            "Mieszanie z Konfiguracjami".ship_date,
            "Mieszanie z Konfiguracjami".date_entered,
            "Mieszanie z Konfiguracjami".id,
            "Mieszanie z Konfiguracjami".prod,
            "Mieszanie z Konfiguracjami".prod_week,
            "Mieszanie z Konfiguracjami".planner_buyer,
            "Mieszanie z Konfiguracjami".indeks,
            "Mieszanie z Konfiguracjami".opis,
            "Mieszanie z Konfiguracjami".typ_zdarzenia,
            "Mieszanie z Konfiguracjami".status_informacji,
            "Mieszanie z Konfiguracjami".zest
           FROM public."Mieszanie z Konfiguracjami"
        UNION ALL
         SELECT ('D'::text || to_char(a_1.dop_id, '9999999999'::text)) AS ordid,
            a_1.dop_id AS dop,
            a_1.koor,
            a_1.order_no,
            a_1.line_no,
            a_1.rel_no,
            a_1.part_no,
            a_1.descr,
            a_1.country,
            a_1.prom_date,
            (a_1.prom_week)::integer AS prom_week,
            a_1.load_id,
            a_1.ship_date,
            a_1.date_entered,
            a_1.id,
            a_1.data_dop AS prod,
            (
                CASE
                    WHEN (date_part('isodow'::text, a_1.data_dop) < (public.shipment_day(a_1.country, a_1.cust_no, a_1.zip_code, a_1.addr1))::double precision) THEN to_char((a_1.data_dop)::timestamp with time zone, 'IYYYIW'::text)
                    ELSE to_char((a_1.data_dop + '7 days'::interval), 'IYYYIW'::text)
                END)::integer AS prod_week,
            'RADKOS'::text AS planner_buyer,
            'BŁĄD POTW.'::text AS indeks,
            'Zła data obiecana (Bieżąca lub na wstecz)'::text AS opis,
            'Wewn. procedura'::text AS typ_zdarzenia,
            'POPRAWIĆ'::text AS status_informacji,
            a_1.zest
           FROM (( SELECT cust_ord.koor,
                    cust_ord.order_no,
                    cust_ord.line_no,
                    cust_ord.rel_no,
                    cust_ord.line_item_no,
                    cust_ord.last_mail_conf,
                    cust_ord.state_conf,
                    cust_ord.line_state,
                    cust_ord.cust_order_state,
                    cust_ord.country,
                    cust_ord.cust_no,
                    cust_ord.zip_code,
                    cust_ord.addr1,
                    cust_ord.prom_date,
                    cust_ord.prom_week,
                    cust_ord.load_id,
                    cust_ord.ship_date,
                    cust_ord.part_no,
                    cust_ord.descr,
                    cust_ord.configuration,
                    cust_ord.buy_qty_due,
                    cust_ord.desired_qty,
                    cust_ord.qty_invoiced,
                    cust_ord.qty_shipped,
                    cust_ord.qty_assigned,
                    cust_ord.dop_connection_db,
                    cust_ord.dop_id,
                    cust_ord.dop_state,
                    cust_ord.data_dop,
                    cust_ord.dop_qty,
                    cust_ord.dop_made,
                    cust_ord.date_entered,
                    cust_ord.chksum,
                    cust_ord.custid,
                    cust_ord.id,
                    cust_ord.zest,
                    cust_ord.seria0,
                    cust_ord.data0,
                    cust_ord.objversion
                   FROM public.cust_ord
                  WHERE ((cust_ord.seria0 <> true) AND ((cust_ord.data0 IS NULL) OR ((cust_ord.data0 IS NOT NULL) AND (cust_ord.seria0 = false) AND (public.wrk_day(public.wrk_near_count(((cust_ord.data0 + '21 days'::interval))::date)) < cust_ord.data_dop))) AND ((cust_ord.dop_state)::text <> 'Released'::text) AND ((((cust_ord.prom_week)::integer <= (to_char((('now'::text)::date)::timestamp with time zone, 'IYYYIW'::text))::integer) AND (substr((cust_ord.order_no)::text, 1, 1) = 'S'::text)) OR (((cust_ord.prom_week)::integer < (to_char((('now'::text)::date)::timestamp with time zone, 'IYYYIW'::text))::integer) AND (substr((cust_ord.order_no)::text, 1, 1) <> 'S'::text))))) a_1
             LEFT JOIN public.braki b ON (((COALESCE(b.zest, (b.ordid)::character varying))::text = (COALESCE(a_1.zest, (('D'::text || to_char(a_1.dop_id, '9999999999'::text)))::character varying))::text)))
          WHERE (b.id IS NULL)) a
  WITH NO DATA;


ALTER TABLE public.to_mail OWNER TO postgres;

--
-- TOC entry 257 (class 1259 OID 17456)
-- Name: type_dmd; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.type_dmd (
    id integer NOT NULL,
    type_dmd character varying(15)
);


ALTER TABLE public.type_dmd OWNER TO postgres;

--
-- TOC entry 258 (class 1259 OID 17459)
-- Name: users_indb; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.users_indb AS
 SELECT u.usename AS "User name",
    u.usesysid AS "User ID",
        CASE
            WHEN (u.usesuper AND u.usecreatedb) THEN 'superuser, create
database'::text
            WHEN u.usesuper THEN 'superuser'::text
            WHEN u.usecreatedb THEN 'create database'::text
            ELSE ''::text
        END AS "Attributes"
   FROM pg_user u;


ALTER TABLE public.users_indb OWNER TO postgres;

--
-- TOC entry 260 (class 1259 OID 26249)
-- Name: zak_dat; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.zak_dat AS
 SELECT data.indeks,
    data.umiejsc,
    data.opis,
    data.kolekcja,
    data.mag,
    data.planner_buyer,
    data.rodzaj,
    public.dmd_type(data.przyczyna) AS dmd_type,
    data.czas_dostawy,
    data.data_gwarancji AS date,
    data.data_dost,
    data.wlk_dost,
    data.bilans,
    data.data_braku,
    data."bil_dost_dzień",
    data.typ_zdarzenia,
    data.widoczny_od_dnia,
    data.sum_dost,
    data.sum_potrz,
    data."sum_dost_opóźnion",
    data."sum_potrz_opóźnion",
    data.status_informacji,
    data.refr_date,
    data.informacja AS "Informacja"
   FROM public.data
  ORDER BY data.indeks, data.data_dost;


ALTER TABLE public.zak_dat OWNER TO postgres;

--
-- TOC entry 3447 (class 2606 OID 17743)
-- Name: CRP CRP_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public."CRP"
    ADD CONSTRAINT "CRP_pkey" PRIMARY KEY (id);


--
-- TOC entry 3469 (class 2606 OID 17745)
-- Name: Past_day Past_day_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public."Past_day"
    ADD CONSTRAINT "Past_day_pkey" PRIMARY KEY (objversion);


--
-- TOC entry 3457 (class 2606 OID 17747)
-- Name: braki braki_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.braki
    ADD CONSTRAINT braki_pkey PRIMARY KEY (id);


--
-- TOC entry 3494 (class 2606 OID 17749)
-- Name: braki_tmp braki_pkey_tmp; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.braki_tmp
    ADD CONSTRAINT braki_pkey_tmp PRIMARY KEY (id);


--
-- TOC entry 3546 (class 2606 OID 17958)
-- Name: contract_calendar contract_calendar_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.contract_calendar
    ADD CONSTRAINT contract_calendar_pkey PRIMARY KEY (contract);


--
-- TOC entry 3451 (class 2606 OID 17751)
-- Name: cust_ord cust_ord_pkey_n; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.cust_ord
    ADD CONSTRAINT cust_ord_pkey_n PRIMARY KEY (id);


--
-- TOC entry 3503 (class 2606 OID 17753)
-- Name: data data_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.data
    ADD CONSTRAINT data_pkey PRIMARY KEY (id);


--
-- TOC entry 3505 (class 2606 OID 17755)
-- Name: datatbles datatbles_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.datatbles
    ADD CONSTRAINT datatbles_pkey PRIMARY KEY (table_name);


--
-- TOC entry 3508 (class 2606 OID 17757)
-- Name: day_qty_ifs day_qty_ifs_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.day_qty_ifs
    ADD CONSTRAINT day_qty_ifs_pkey PRIMARY KEY (id);


--
-- TOC entry 3474 (class 2606 OID 17759)
-- Name: demands demands_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.demands
    ADD CONSTRAINT demands_pkey PRIMARY KEY (id);


--
-- TOC entry 3514 (class 2606 OID 17761)
-- Name: kontakty kontakty_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.kontakty
    ADD CONSTRAINT kontakty_pkey PRIMARY KEY (id);


--
-- TOC entry 3522 (class 2606 OID 17763)
-- Name: lack_ord_demands lack_ord_demands_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.lack_ord_demands
    ADD CONSTRAINT lack_ord_demands_pkey PRIMARY KEY (id);


--
-- TOC entry 3525 (class 2606 OID 17765)
-- Name: late_ord late_ordpkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.late_ord
    ADD CONSTRAINT late_ordpkey PRIMARY KEY (cust_id);


--
-- TOC entry 3476 (class 2606 OID 17987)
-- Name: mag mag_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.mag
    ADD CONSTRAINT mag_pkey PRIMARY KEY (note_id);


--
-- TOC entry 3499 (class 2606 OID 17769)
-- Name: mail mail_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.mail
    ADD CONSTRAINT mail_pkey PRIMARY KEY (cust_id);


--
-- TOC entry 3531 (class 2606 OID 17771)
-- Name: ord_demands ord_demands_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ord_demands
    ADD CONSTRAINT ord_demands_pkey PRIMARY KEY (id);


--
-- TOC entry 3487 (class 2606 OID 17773)
-- Name: ord_lack ord_demands_pkey_lack; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ord_lack
    ADD CONSTRAINT ord_demands_pkey_lack PRIMARY KEY (id);


--
-- TOC entry 3519 (class 2606 OID 17775)
-- Name: ord_lack_bil ord_demands_pkey_lack_bil; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ord_lack_bil
    ADD CONSTRAINT ord_demands_pkey_lack_bil PRIMARY KEY (id);


--
-- TOC entry 3512 (class 2606 OID 17777)
-- Name: potw potw_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.potw
    ADD CONSTRAINT potw_pkey PRIMARY KEY (id);


--
-- TOC entry 3536 (class 2606 OID 17779)
-- Name: server_query server_query_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.server_query
    ADD CONSTRAINT server_query_pkey PRIMARY KEY (id);


--
-- TOC entry 3540 (class 2606 OID 17781)
-- Name: shop_ord shop_ord_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.shop_ord
    ADD CONSTRAINT shop_ord_pkey PRIMARY KEY (id);


--
-- TOC entry 3543 (class 2606 OID 17783)
-- Name: type_dmd type_dmd_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.type_dmd
    ADD CONSTRAINT type_dmd_pkey PRIMARY KEY (id);


--
-- TOC entry 3470 (class 1259 OID 27949)
-- Name: Contract_part_no; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX "Contract_part_no" ON public.demands USING btree (part_no bpchar_pattern_ops, contract bpchar_pattern_ops) WITH (deduplicate_items='true');


--
-- TOC entry 3526 (class 1259 OID 27952)
-- Name: Contract_part_no_date_req; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX "Contract_part_no_date_req" ON public.ord_demands USING btree (part_no bpchar_pattern_ops, contract bpchar_pattern_ops, date_required) WITH (deduplicate_items='true');


--
-- TOC entry 3489 (class 1259 OID 17786)
-- Name: Counter; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX "Counter" ON public.work_cal USING hash (counter);


--
-- TOC entry 3448 (class 1259 OID 17787)
-- Name: Cust_order_line_n; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX "Cust_order_line_n" ON public.cust_ord USING btree (order_no, line_no, rel_no, line_item_no);


--
-- TOC entry 3460 (class 1259 OID 17788)
-- Name: Cust_order_line_n_hist; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX "Cust_order_line_n_hist" ON public.cust_ord_history USING btree (order_no, line_no, rel_no, line_item_no);


--
-- TOC entry 3449 (class 1259 OID 17789)
-- Name: Cust_order_n; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX "Cust_order_n" ON public.cust_ord USING btree (order_no);


--
-- TOC entry 3461 (class 1259 OID 17790)
-- Name: Cust_order_n_hist; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX "Cust_order_n_hist" ON public.cust_ord_history USING btree (order_no);


--
-- TOC entry 3471 (class 1259 OID 17791)
-- Name: ID; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX "ID" ON public.demands USING hash (id);


--
-- TOC entry 3500 (class 1259 OID 17792)
-- Name: ID_DMD; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX "ID_DMD" ON public.data USING hash (id);


--
-- TOC entry 3527 (class 1259 OID 17793)
-- Name: Id_ord; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX "Id_ord" ON public.ord_demands USING hash (id);


--
-- TOC entry 3483 (class 1259 OID 17794)
-- Name: Id_ord_lack; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX "Id_ord_lack" ON public.ord_lack USING hash (id);


--
-- TOC entry 3515 (class 1259 OID 17795)
-- Name: Id_ord_lack_bil; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX "Id_ord_lack_bil" ON public.ord_lack_bil USING hash (id);


--
-- TOC entry 3462 (class 1259 OID 17796)
-- Name: Ord_hist_dateAdd; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX "Ord_hist_dateAdd" ON public.cust_ord_history USING btree (date_add);


--
-- TOC entry 3472 (class 1259 OID 17797)
-- Name: PART_NO; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX "PART_NO" ON public.demands USING btree (part_no, work_day);


--
-- TOC entry 3501 (class 1259 OID 17798)
-- Name: PART_NO_DAT; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX "PART_NO_DAT" ON public.data USING btree (indeks, data_braku);


--
-- TOC entry 3509 (class 1259 OID 17799)
-- Name: POTW_ID; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX "POTW_ID" ON public.potw USING hash (id);


--
-- TOC entry 3528 (class 1259 OID 17800)
-- Name: REFR_ord; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX "REFR_ord" ON public.ord_demands USING btree (part_no, date_required);


--
-- TOC entry 3484 (class 1259 OID 17801)
-- Name: REFR_ord_lack; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX "REFR_ord_lack" ON public.ord_lack USING btree (part_no, date_required);


--
-- TOC entry 3516 (class 1259 OID 17802)
-- Name: REFR_ord_lack_bil; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX "REFR_ord_lack_bil" ON public.ord_lack_bil USING btree (part_no, date_required);


--
-- TOC entry 3529 (class 1259 OID 17803)
-- Name: Sel_ord; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX "Sel_ord" ON public.ord_demands USING btree (dop DESC, order_no, dop_lin, line_no, rel_no, int_ord);


--
-- TOC entry 3485 (class 1259 OID 17804)
-- Name: Sel_ord_lack; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX "Sel_ord_lack" ON public.ord_lack USING btree (dop DESC, order_no, dop_lin, line_no, rel_no, int_ord);


--
-- TOC entry 3517 (class 1259 OID 17805)
-- Name: Sel_ord_lack_bil; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX "Sel_ord_lack_bil" ON public.ord_lack_bil USING btree (dop DESC, order_no, dop_lin, line_no, rel_no, int_ord);


--
-- TOC entry 3490 (class 1259 OID 17806)
-- Name: WORK_DAY; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX "WORK_DAY" ON public.work_cal USING hash (work_day);


--
-- TOC entry 3455 (class 1259 OID 17807)
-- Name: braki_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX braki_id ON public.braki USING hash (id);


--
-- TOC entry 3495 (class 1259 OID 17808)
-- Name: braki_tmp_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX braki_tmp_id ON public.braki_tmp USING hash (id);


--
-- TOC entry 3491 (class 1259 OID 17967)
-- Name: calendar_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX calendar_id ON public.work_cal USING btree (calendar_id, work_day) INCLUDE (calendar_id, work_day) WITH (deduplicate_items='true');


--
-- TOC entry 3544 (class 1259 OID 27950)
-- Name: contract; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX contract ON public.contract_calendar USING btree (contract bpchar_pattern_ops) WITH (deduplicate_items='true');


--
-- TOC entry 3492 (class 1259 OID 17968)
-- Name: contract_calendar_ind; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX contract_calendar_ind ON public.work_cal USING btree (calendar_id, counter) INCLUDE (calendar_id, counter) WITH (deduplicate_items='true');


--
-- TOC entry 3479 (class 1259 OID 17809)
-- Name: cust_linid; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX cust_linid ON public.braki_hist USING hash (cust_id);


--
-- TOC entry 3452 (class 1259 OID 17810)
-- Name: custind_n; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX custind_n ON public.cust_ord USING btree (custid);


--
-- TOC entry 3463 (class 1259 OID 17811)
-- Name: custind_n_hist; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX custind_n_hist ON public.cust_ord_history USING btree (custid);


--
-- TOC entry 3480 (class 1259 OID 17812)
-- Name: date_add; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX date_add ON public.braki_hist USING btree (objversion DESC NULLS LAST);


--
-- TOC entry 3458 (class 1259 OID 17813)
-- Name: dop_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX dop_id ON public.braki USING btree (dop);


--
-- TOC entry 3453 (class 1259 OID 17814)
-- Name: dop_id_n; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX dop_id_n ON public.cust_ord USING hash (dop_id);


--
-- TOC entry 3464 (class 1259 OID 17815)
-- Name: dop_id_n_hist; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX dop_id_n_hist ON public.cust_ord_history USING hash (dop_id);


--
-- TOC entry 3481 (class 1259 OID 17816)
-- Name: id_in_table; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX id_in_table ON public.braki_hist USING hash (id);


--
-- TOC entry 3541 (class 1259 OID 17817)
-- Name: id_type; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX id_type ON public.type_dmd USING hash (id);


--
-- TOC entry 3523 (class 1259 OID 17818)
-- Name: late_ord_dop; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX late_ord_dop ON public.late_ord USING btree (dop);


--
-- TOC entry 3466 (class 1259 OID 17819)
-- Name: mail_hist_dateAddd; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX "mail_hist_dateAddd" ON public.mail_hist USING btree (date_addd);


--
-- TOC entry 3497 (class 1259 OID 17820)
-- Name: mail_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX mail_id ON public.mail USING hash (cust_id);


--
-- TOC entry 3467 (class 1259 OID 17821)
-- Name: mail_id_hist; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX mail_id_hist ON public.mail_hist USING hash (cust_id);


--
-- TOC entry 3477 (class 1259 OID 17822)
-- Name: note_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX note_id ON public.mag USING btree (note_id);


--
-- TOC entry 3532 (class 1259 OID 17824)
-- Name: order_no_line_no_rel_no; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX order_no_line_no_rel_no ON public.ord_demands USING btree (order_no, line_no, rel_no);


--
-- TOC entry 3488 (class 1259 OID 17825)
-- Name: order_no_line_no_rel_no_lack; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX order_no_line_no_rel_no_lack ON public.ord_lack USING btree (order_no, line_no, rel_no);


--
-- TOC entry 3520 (class 1259 OID 17826)
-- Name: order_no_line_no_rel_no_lack_bil; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX order_no_line_no_rel_no_lack_bil ON public.ord_lack_bil USING btree (order_no, line_no, rel_no);


--
-- TOC entry 3454 (class 1259 OID 17827)
-- Name: order_noline_norel_no_n; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX order_noline_norel_no_n ON public.cust_ord USING btree (order_no, line_no, rel_no);


--
-- TOC entry 3465 (class 1259 OID 17828)
-- Name: order_noline_norel_no_n_hist; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX order_noline_norel_no_n_hist ON public.cust_ord_history USING btree (order_no, line_no, rel_no);


--
-- TOC entry 3478 (class 1259 OID 17829)
-- Name: part_no; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX part_no ON public.mag USING hash (indeks);


--
-- TOC entry 3533 (class 1259 OID 27951)
-- Name: part_no_contract; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX part_no_contract ON public.ord_demands USING btree (part_no bpchar_pattern_ops, contract bpchar_pattern_ops) WITH (deduplicate_items='true');


--
-- TOC entry 3510 (class 1259 OID 17830)
-- Name: potw_ind_dost; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX potw_ind_dost ON public.potw USING btree (indeks, data_dost);


--
-- TOC entry 3459 (class 1259 OID 17831)
-- Name: quer_refr; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX quer_refr ON public.braki USING btree (max_prod_date DESC, indeks, data_dost, sort_ord DESC);


--
-- TOC entry 3482 (class 1259 OID 17832)
-- Name: quer_refr_hist; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX quer_refr_hist ON public.braki_hist USING btree (max_prod_date DESC, indeks, data_dost, sort_ord DESC);


--
-- TOC entry 3496 (class 1259 OID 17833)
-- Name: quer_refr_tmp; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX quer_refr_tmp ON public.braki_tmp USING btree (max_prod_date DESC, indeks, data_dost, sort_ord DESC);


--
-- TOC entry 3534 (class 1259 OID 17834)
-- Name: ser_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ser_id ON public.server_query USING hash (id);


--
-- TOC entry 3538 (class 1259 OID 17835)
-- Name: shop_ord_dop; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX shop_ord_dop ON public.shop_ord USING btree (dop);


--
-- TOC entry 3537 (class 1259 OID 17836)
-- Name: start_date; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX start_date ON public.server_query USING btree (start_date);


--
-- TOC entry 3506 (class 1259 OID 17837)
-- Name: tables_nam; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX tables_nam ON public.datatbles USING hash (table_name);


--
-- TOC entry 3549 (class 2620 OID 17838)
-- Name: day_qty NOTNULL; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER "NOTNULL" BEFORE INSERT OR UPDATE ON public.day_qty FOR EACH ROW EXECUTE FUNCTION public.day_qty_notnull();


--
-- TOC entry 3556 (class 2620 OID 17839)
-- Name: server_query TMP_ommit_err; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER "TMP_ommit_err" BEFORE INSERT ON public.server_query FOR EACH ROW EXECUTE FUNCTION public.refr_views_tmp();

ALTER TABLE public.server_query DISABLE TRIGGER "TMP_ommit_err";


--
-- TOC entry 3554 (class 2620 OID 17840)
-- Name: datatbles Updt_dayQTY; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER "Updt_dayQTY" BEFORE UPDATE ON public.datatbles FOR EACH ROW EXECUTE FUNCTION public."Empty_notexist"();


--
-- TOC entry 3550 (class 2620 OID 17841)
-- Name: day_qty div_zero; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER div_zero AFTER INSERT OR UPDATE ON public.day_qty FOR EACH STATEMENT EXECUTE FUNCTION public.div_zero_day_qty();


--
-- TOC entry 3551 (class 2620 OID 17842)
-- Name: mod_date if_err_update_dmds; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER if_err_update_dmds BEFORE INSERT ON public.mod_date FOR EACH ROW EXECUTE FUNCTION public.try_update_orders();


--
-- TOC entry 3547 (class 2620 OID 17843)
-- Name: cust_ord mod_rec; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER mod_rec BEFORE UPDATE ON public.cust_ord FOR EACH ROW EXECUTE FUNCTION public.mod_ord_confirm_date();


--
-- TOC entry 3852 (class 0 OID 0)
-- Dependencies: 3547
-- Name: TRIGGER mod_rec ON cust_ord; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TRIGGER mod_rec ON public.cust_ord IS 'Wyzwalacz uruchamiany dla celów historii dla śledzenia zmian ,które nie są poprawnie gromadzone w IFS';


--
-- TOC entry 3555 (class 2620 OID 17844)
-- Name: datatbles nota; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER nota AFTER UPDATE ON public.datatbles FOR EACH STATEMENT EXECUTE FUNCTION public.note();


--
-- TOC entry 3552 (class 2620 OID 17845)
-- Name: mail to_hist; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER to_hist BEFORE INSERT OR DELETE OR UPDATE ON public.mail FOR EACH ROW EXECUTE FUNCTION public."Hist_mail"();


--
-- TOC entry 3553 (class 2620 OID 17846)
-- Name: send_mail to_hist_send; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER to_hist_send BEFORE UPDATE ON public.send_mail FOR EACH ROW EXECUTE FUNCTION public.hist_send_mail();


--
-- TOC entry 3548 (class 2620 OID 17847)
-- Name: braki to_histbrak; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER to_histbrak BEFORE INSERT OR DELETE OR UPDATE ON public.braki FOR EACH ROW EXECUTE FUNCTION public."Hist_braki"();


--
-- TOC entry 3725 (class 0 OID 0)
-- Dependencies: 333
-- Name: FUNCTION "Empty_notexist"(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public."Empty_notexist"() TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3726 (class 0 OID 0)
-- Dependencies: 342
-- Name: FUNCTION "Hist_braki"(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public."Hist_braki"() TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3727 (class 0 OID 0)
-- Dependencies: 334
-- Name: FUNCTION "Hist_mail"(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public."Hist_mail"() TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3728 (class 0 OID 0)
-- Dependencies: 289
-- Name: FUNCTION addinfo_purch(indeks character varying, data_dost date, info character varying, dost_ilosc double precision); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.addinfo_purch(indeks character varying, data_dost date, info character varying, dost_ilosc double precision) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3729 (class 0 OID 0)
-- Dependencies: 290
-- Name: FUNCTION calendar_id(contract character varying); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.calendar_id(contract character varying) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3730 (class 0 OID 0)
-- Dependencies: 291
-- Name: FUNCTION confirm_purch(indeks character varying, dost_ilosc double precision, data_dost date, rodzaj_potw character varying, termin_wazn date, koor character varying, sum_dost double precision); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.confirm_purch(indeks character varying, dost_ilosc double precision, data_dost date, rodzaj_potw character varying, termin_wazn date, koor character varying, sum_dost double precision) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3731 (class 0 OID 0)
-- Dependencies: 292
-- Name: FUNCTION cust_lin_stat(cust_id uuid); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.cust_lin_stat(cust_id uuid) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3733 (class 0 OID 0)
-- Dependencies: 293
-- Name: FUNCTION date_fromnow("DAYS" integer); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.date_fromnow("DAYS" integer) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3734 (class 0 OID 0)
-- Dependencies: 303
-- Name: FUNCTION date_fromnow("DAYS" integer, contract character varying); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.date_fromnow("DAYS" integer, contract character varying) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3735 (class 0 OID 0)
-- Dependencies: 294
-- Name: FUNCTION date_ser0(cust_id uuid); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.date_ser0(cust_id uuid) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3737 (class 0 OID 0)
-- Dependencies: 295
-- Name: FUNCTION date_shift_days("FROM_DAYS" date, "SHIFT_DAYS" integer); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.date_shift_days("FROM_DAYS" date, "SHIFT_DAYS" integer) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3738 (class 0 OID 0)
-- Dependencies: 304
-- Name: FUNCTION date_shift_days("FROM_DAYS" date, "SHIFT_DAYS" integer, contract character varying); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.date_shift_days("FROM_DAYS" date, "SHIFT_DAYS" integer, contract character varying) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3739 (class 0 OID 0)
-- Dependencies: 335
-- Name: FUNCTION day_qty_notnull(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.day_qty_notnull() TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3740 (class 0 OID 0)
-- Dependencies: 336
-- Name: FUNCTION div_zero_day_qty(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.div_zero_day_qty() TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3742 (class 0 OID 0)
-- Dependencies: 296
-- Name: FUNCTION dmd_type("TYPE_code" integer); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.dmd_type("TYPE_code" integer) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3743 (class 0 OID 0)
-- Dependencies: 343
-- Name: FUNCTION get_contract_from_dop("DOP_ID" integer); Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON FUNCTION public.get_contract_from_dop("DOP_ID" integer) FROM postgres;
GRANT ALL ON FUNCTION public.get_contract_from_dop("DOP_ID" integer) TO postgres WITH GRANT OPTION;
GRANT ALL ON FUNCTION public.get_contract_from_dop("DOP_ID" integer) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3744 (class 0 OID 0)
-- Dependencies: 297
-- Name: FUNCTION get_date_dop(cust_id uuid); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.get_date_dop(cust_id uuid) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3745 (class 0 OID 0)
-- Dependencies: 298
-- Name: FUNCTION get_dopstat(cust_id uuid); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.get_dopstat(cust_id uuid) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3746 (class 0 OID 0)
-- Dependencies: 316
-- Name: FUNCTION get_inventory(part_no character varying); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.get_inventory(part_no character varying) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3747 (class 0 OID 0)
-- Dependencies: 299
-- Name: FUNCTION get_koor(part_no character varying); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.get_koor(part_no character varying) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3748 (class 0 OID 0)
-- Dependencies: 300
-- Name: FUNCTION get_refer(addr1 character varying); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.get_refer(addr1 character varying) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3749 (class 0 OID 0)
-- Dependencies: 345
-- Name: FUNCTION getltfrompart(descr character varying); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.getltfrompart(descr character varying) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3750 (class 0 OID 0)
-- Dependencies: 337
-- Name: FUNCTION hist_send_mail(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.hist_send_mail() TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3751 (class 0 OID 0)
-- Dependencies: 305
-- Name: FUNCTION how_many(dop integer); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.how_many(dop integer) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3752 (class 0 OID 0)
-- Dependencies: 306
-- Name: FUNCTION instr(character varying, character varying); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.instr(character varying, character varying) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3753 (class 0 OID 0)
-- Dependencies: 307
-- Name: FUNCTION instr(character varying, character varying, integer); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.instr(character varying, character varying, integer) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3754 (class 0 OID 0)
-- Dependencies: 308
-- Name: FUNCTION instr(character varying, character varying, integer, integer); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.instr(character varying, character varying, integer, integer) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3755 (class 0 OID 0)
-- Dependencies: 309
-- Name: FUNCTION is_alter(status_informacji character varying); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.is_alter(status_informacji character varying) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3756 (class 0 OID 0)
-- Dependencies: 310
-- Name: FUNCTION is_confirm(status_informacji character varying); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.is_confirm(status_informacji character varying) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3757 (class 0 OID 0)
-- Dependencies: 311
-- Name: FUNCTION is_dontpurch(status_informacji character varying); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.is_dontpurch(status_informacji character varying) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3758 (class 0 OID 0)
-- Dependencies: 312
-- Name: FUNCTION is_for_mail(status_informacji character varying); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.is_for_mail(status_informacji character varying) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3759 (class 0 OID 0)
-- Dependencies: 313
-- Name: FUNCTION is_interncl(cust_id uuid); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.is_interncl(cust_id uuid) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3760 (class 0 OID 0)
-- Dependencies: 314
-- Name: FUNCTION is_refer(addr1 character varying); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.is_refer(addr1 character varying) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3761 (class 0 OID 0)
-- Dependencies: 315
-- Name: FUNCTION is_seria0(cust_id uuid); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.is_seria0(cust_id uuid) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3762 (class 0 OID 0)
-- Dependencies: 271
-- Name: FUNCTION late_ord_exist(cust_id uuid); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.late_ord_exist(cust_id uuid) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3763 (class 0 OID 0)
-- Dependencies: 272
-- Name: FUNCTION mag_type(indeks character varying); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.mag_type(indeks character varying) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3765 (class 0 OID 0)
-- Dependencies: 338
-- Name: FUNCTION mod_ord_confirm_date(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.mod_ord_confirm_date() TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3766 (class 0 OID 0)
-- Dependencies: 339
-- Name: FUNCTION note(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.note() TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3768 (class 0 OID 0)
-- Dependencies: 286
-- Name: FUNCTION now_counter(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.now_counter() TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3769 (class 0 OID 0)
-- Dependencies: 273
-- Name: FUNCTION now_counter(contract character varying); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.now_counter(contract character varying) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3771 (class 0 OID 0)
-- Dependencies: 274
-- Name: FUNCTION now_near_counter(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.now_near_counter() TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3772 (class 0 OID 0)
-- Dependencies: 301
-- Name: FUNCTION now_near_counter(contract character varying); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.now_near_counter(contract character varying) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3773 (class 0 OID 0)
-- Dependencies: 287
-- Name: FUNCTION ord_objver(cust_id uuid); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.ord_objver(cust_id uuid) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3774 (class 0 OID 0)
-- Dependencies: 340
-- Name: FUNCTION refr_views_tmp(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.refr_views_tmp() TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3775 (class 0 OID 0)
-- Dependencies: 288
-- Name: FUNCTION server_state(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.server_state() TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3776 (class 0 OID 0)
-- Dependencies: 302
-- Name: FUNCTION server_updt(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.server_updt() TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3778 (class 0 OID 0)
-- Dependencies: 317
-- Name: FUNCTION shipment_day(country_code character varying, cust_no character varying, zip_code character varying, addr1 character varying); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.shipment_day(country_code character varying, cust_no character varying, zip_code character varying, addr1 character varying) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3780 (class 0 OID 0)
-- Dependencies: 341
-- Name: FUNCTION try_update_orders(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.try_update_orders() TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3781 (class 0 OID 0)
-- Dependencies: 318
-- Name: FUNCTION updt_dta_potw(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.updt_dta_potw() TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3782 (class 0 OID 0)
-- Dependencies: 319
-- Name: FUNCTION uuid_generate_v1(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.uuid_generate_v1() TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3783 (class 0 OID 0)
-- Dependencies: 320
-- Name: FUNCTION uuid_generate_v1mc(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.uuid_generate_v1mc() TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3784 (class 0 OID 0)
-- Dependencies: 321
-- Name: FUNCTION uuid_generate_v3(namespace uuid, name text); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.uuid_generate_v3(namespace uuid, name text) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3785 (class 0 OID 0)
-- Dependencies: 322
-- Name: FUNCTION uuid_generate_v4(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.uuid_generate_v4() TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3786 (class 0 OID 0)
-- Dependencies: 323
-- Name: FUNCTION uuid_generate_v5(namespace uuid, name text); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.uuid_generate_v5(namespace uuid, name text) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3787 (class 0 OID 0)
-- Dependencies: 324
-- Name: FUNCTION uuid_nil(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.uuid_nil() TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3788 (class 0 OID 0)
-- Dependencies: 325
-- Name: FUNCTION uuid_ns_dns(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.uuid_ns_dns() TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3789 (class 0 OID 0)
-- Dependencies: 326
-- Name: FUNCTION uuid_ns_oid(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.uuid_ns_oid() TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3790 (class 0 OID 0)
-- Dependencies: 327
-- Name: FUNCTION uuid_ns_url(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.uuid_ns_url() TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3791 (class 0 OID 0)
-- Dependencies: 328
-- Name: FUNCTION uuid_ns_x500(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.uuid_ns_x500() TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3792 (class 0 OID 0)
-- Dependencies: 329
-- Name: FUNCTION weektodat(week character varying); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.weektodat(week character varying) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3794 (class 0 OID 0)
-- Dependencies: 330
-- Name: FUNCTION wrk_count("WORK_DAY" date); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.wrk_count("WORK_DAY" date) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3795 (class 0 OID 0)
-- Dependencies: 331
-- Name: FUNCTION wrk_count("WORK_DAY" date, contract character varying); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.wrk_count("WORK_DAY" date, contract character varying) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3797 (class 0 OID 0)
-- Dependencies: 332
-- Name: FUNCTION wrk_day("COUNTER" integer); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.wrk_day("COUNTER" integer) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3798 (class 0 OID 0)
-- Dependencies: 268
-- Name: FUNCTION wrk_day("COUNTER" integer, contract character varying); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.wrk_day("COUNTER" integer, contract character varying) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3800 (class 0 OID 0)
-- Dependencies: 270
-- Name: FUNCTION wrk_near_count("WORK_DAY" date); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.wrk_near_count("WORK_DAY" date) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3801 (class 0 OID 0)
-- Dependencies: 267
-- Name: FUNCTION wrk_near_count("WORK_DAY" date, "CONTRACT" character varying); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.wrk_near_count("WORK_DAY" date, "CONTRACT" character varying) TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3803 (class 0 OID 0)
-- Dependencies: 215
-- Name: TABLE "CRP"; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public."CRP" TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3804 (class 0 OID 0)
-- Dependencies: 216
-- Name: TABLE cust_ord; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.cust_ord TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3805 (class 0 OID 0)
-- Dependencies: 217
-- Name: TABLE send_mail_hist; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.send_mail_hist TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3806 (class 0 OID 0)
-- Dependencies: 218
-- Name: TABLE "Liczba zmain terminów "; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public."Liczba zmain terminów " TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3808 (class 0 OID 0)
-- Dependencies: 219
-- Name: TABLE braki; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.braki TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3809 (class 0 OID 0)
-- Dependencies: 220
-- Name: TABLE cust_ord_history; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.cust_ord_history TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3810 (class 0 OID 0)
-- Dependencies: 221
-- Name: TABLE mail_hist; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.mail_hist TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3811 (class 0 OID 0)
-- Dependencies: 222
-- Name: TABLE "Mieszanie z Konfiguracjami"; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public."Mieszanie z Konfiguracjami" TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3813 (class 0 OID 0)
-- Dependencies: 223
-- Name: TABLE "Past_day"; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public."Past_day" TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3814 (class 0 OID 0)
-- Dependencies: 224
-- Name: TABLE active_locks; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.active_locks TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3815 (class 0 OID 0)
-- Dependencies: 225
-- Name: TABLE demands; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.demands TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3816 (class 0 OID 0)
-- Dependencies: 226
-- Name: TABLE aktual_hist; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.aktual_hist TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3817 (class 0 OID 0)
-- Dependencies: 227
-- Name: TABLE mag; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.mag TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3818 (class 0 OID 0)
-- Dependencies: 228
-- Name: TABLE day_qty; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.day_qty TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3819 (class 0 OID 0)
-- Dependencies: 229
-- Name: TABLE braki_hist; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.braki_hist TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3820 (class 0 OID 0)
-- Dependencies: 230
-- Name: TABLE ord_lack; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.ord_lack TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3821 (class 0 OID 0)
-- Dependencies: 231
-- Name: TABLE mod_date; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.mod_date TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3822 (class 0 OID 0)
-- Dependencies: 232
-- Name: TABLE work_cal; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.work_cal TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3824 (class 0 OID 0)
-- Dependencies: 233
-- Name: TABLE braki_tmp; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.braki_tmp TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3825 (class 0 OID 0)
-- Dependencies: 234
-- Name: TABLE conf_mail_null; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.conf_mail_null TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3826 (class 0 OID 0)
-- Dependencies: 235
-- Name: TABLE mail; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.mail TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3827 (class 0 OID 0)
-- Dependencies: 236
-- Name: TABLE send_mail; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.send_mail TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3828 (class 0 OID 0)
-- Dependencies: 237
-- Name: TABLE confirm_ord; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.confirm_ord TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3829 (class 0 OID 0)
-- Dependencies: 259
-- Name: TABLE contract_calendar; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.contract_calendar TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3830 (class 0 OID 0)
-- Dependencies: 238
-- Name: TABLE cust_odr_conf_mod; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.cust_odr_conf_mod TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3831 (class 0 OID 0)
-- Dependencies: 239
-- Name: TABLE cust_ord_mod_confirm_date; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.cust_ord_mod_confirm_date TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3832 (class 0 OID 0)
-- Dependencies: 240
-- Name: TABLE cust_ord_mod_ship_date; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.cust_ord_mod_ship_date TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3833 (class 0 OID 0)
-- Dependencies: 241
-- Name: TABLE data; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.data TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3834 (class 0 OID 0)
-- Dependencies: 242
-- Name: TABLE datatbles; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.datatbles TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3835 (class 0 OID 0)
-- Dependencies: 243
-- Name: TABLE day_qty_ifs; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.day_qty_ifs TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3836 (class 0 OID 0)
-- Dependencies: 244
-- Name: TABLE demands_view; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.demands_view TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3837 (class 0 OID 0)
-- Dependencies: 245
-- Name: TABLE potw; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.potw TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3838 (class 0 OID 0)
-- Dependencies: 246
-- Name: TABLE info_inpotw; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.info_inpotw TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3839 (class 0 OID 0)
-- Dependencies: 247
-- Name: TABLE kontakty; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.kontakty TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3840 (class 0 OID 0)
-- Dependencies: 248
-- Name: TABLE fill_sendmail; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.fill_sendmail TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3841 (class 0 OID 0)
-- Dependencies: 249
-- Name: TABLE ord_lack_bil; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.ord_lack_bil TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3842 (class 0 OID 0)
-- Dependencies: 250
-- Name: TABLE lack_ord_demands; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.lack_ord_demands TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3843 (class 0 OID 0)
-- Dependencies: 251
-- Name: TABLE late_ord; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.late_ord TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3844 (class 0 OID 0)
-- Dependencies: 252
-- Name: TABLE ord_demands; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.ord_demands TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3845 (class 0 OID 0)
-- Dependencies: 253
-- Name: TABLE serv_idle; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.serv_idle TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3846 (class 0 OID 0)
-- Dependencies: 254
-- Name: TABLE server_query; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.server_query TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3847 (class 0 OID 0)
-- Dependencies: 255
-- Name: TABLE shop_ord; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.shop_ord TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3848 (class 0 OID 0)
-- Dependencies: 256
-- Name: TABLE to_mail; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.to_mail TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3849 (class 0 OID 0)
-- Dependencies: 257
-- Name: TABLE type_dmd; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.type_dmd TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3850 (class 0 OID 0)
-- Dependencies: 258
-- Name: TABLE users_indb; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.users_indb TO "RADKOS" WITH GRANT OPTION;


--
-- TOC entry 3851 (class 0 OID 0)
-- Dependencies: 260
-- Name: TABLE zak_dat; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.zak_dat TO "RADKOS" WITH GRANT OPTION;


-- Completed on 2024-01-08 12:46:26

--
-- PostgreSQL database dump complete
--

