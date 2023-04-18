create table Peers
(
    "Nickname"    varchar not null primary key,
    "Birthday"    date not null
);

create table Friends (
	"ID"    serial primary key,
	"Peer1" VARCHAR not null,
	"Peer2" VARCHAR not null,
	constraint fk_Friends_Peer1 foreign key ("Peer1") references Peers("Nickname"),
	constraint fk_Friends_Peer2 foreign key ("Peer2") references Peers("Nickname"),
	constraint check_nickname_peer check ("Peer1" <> "Peer2")
);

create table Recommendations (
	"ID"                serial primary key,
	"Peer"              VARCHAR not null,
	"RecomendedPeer"    VARCHAR not null,
	constraint fk_Recommendations_Peer foreign key ("Peer") references Peers("Nickname"),
	constraint fk_Recommendations_RecomendedPeer foreign key ("RecomendedPeer") references Peers("Nickname"),
	constraint check_recomend_peer check ("Peer" <> "RecomendedPeer")
);

create table TransferredPoints (
	"ID"            serial primary key,
	"CheckingPeer"  VARCHAR not null,
	"CheckedPeer"   VARCHAR not null,
	"PointsAmount"  int not null,
	constraint fk_TransferredPoints_CheckingPeer foreign key ("CheckingPeer") references Peers("Nickname"),
	constraint fk_TransferredPoints_CheckedPeer foreign key ("CheckedPeer") references Peers("Nickname"),
	constraint check_nick_peer check ("CheckingPeer" <> "CheckedPeer")
);


create table TimeTracking (
	"ID"    serial primary key,
	"Peer"  VARCHAR not null,
	"Date"  DATE not null,
	"Time"  time not null,
	"State" int not null,
	constraint fk_TimeTracking_Peer foreign key ("Peer") references Peers("Nickname"),
	constraint check_TimeTracking check ("State" in ('1', '2'))
);


create table Tasks (
	"Title"       varchar primary key,
	"ParentTask"  varchar,
	"MaxXP"       bigint not null
	,constraint fr_title_task_parents foreign key ("ParentTask") references Tasks("Title")
);


create table Checks (
	"ID"    serial primary key,
	"Peer"  VARCHAR not null,
	"Task"  VARCHAR not null,
	"Date"  DATE not null,
	constraint fk_Checks_Task foreign key ("Task") references Tasks("Title"),
	constraint fk_Checks_Peer foreign key ("Peer") references Peers("Nickname")
);


CREATE TYPE check_status AS ENUM ('Start', 'Success', 'Failure');

create table P2P (
	"ID"            serial not null primary key,
	"Check"         bigint not null,
	"CheckingPeer"  VARCHAR not null,
	"State"         check_status not null,
	"Time"          time not null,
	constraint fk_P2P_CheckingPeer foreign key ("CheckingPeer") references Peers("Nickname"),
	constraint fk_P2P_Check foreign key ("Check") references Checks("ID")
);


create table Verter (
	"ID"    serial primary key,
	"Check" bigint not null,
	"State" check_status not null,
	"Time"  time not null,
	constraint fk_Verter_Check foreign key ("Check") references Checks("ID")
);


create table XP (
	"ID"        serial primary key,
	"Check"     bigint not null,
	"XPAmount"  bigint not null,
	constraint fk_XP_Check foreign key ("Check") references Checks("ID")

);


create or replace procedure export_to_csv ()
as $$
declare
	export_path varchar = '/Users/ddurrand/Desktop/SQL2_Info21_v1.0-0/src/csv_files/backup_new_csv/';
	export_name varchar [] = array['peers', 'friends', 'recommendations', 'transferredpoints', 'timetracking', 'tasks', 'checks', 'p2p', 'verter', 'xp'];
--     export_name varchar [] = array['timetracking'];
begin
	for i in 1..array_length(export_name, 1)
		loop
			execute format ('copy %s to ''%s%s.csv'' with delimiter '','' csv', export_name[i], export_path, export_name[i]);
		end loop;
end;
$$
	language plpgsql;

create or replace procedure import_from_csv ()
as $$
declare
	import_path varchar = '/Users/ddurrand/Desktop/SQL2_Info21_v1.0-0/src/csv_files/new_csv/';
	import_name varchar [] = array['peers', 'friends', 'recommendations', 'transferredpoints', 'timetracking', 'tasks', 'checks', 'p2p', 'verter','xp'];
--     import_name varchar [] = array['timetracking'];
begin
	for i in 1..array_length(import_name, 1)
		loop
			execute format ('copy %s from ''%s%s.csv'' with delimiter '','' csv', import_name[i], import_path, import_name[i]);
		end loop;
end;
$$
	language plpgsql;

call import_from_csv ();

call export_to_csv();

ALTER SEQUENCE "timetracking_ID_seq" RESTART WITH 103;

insert into TimeTracking ("Peer", "Date", "Time", "State") values ('Lastone', (now())::date,
                                                                   '22:25:33', 1);
insert into TimeTracking ("Peer", "Date", "Time", "State") values ('Lastone', (now())::date,
                                                                   '22:45:33', 2);
insert into TimeTracking ("Peer", "Date", "Time", "State") values ('Lastone', (now())::date,
                                                                   '01:15:33', 1);
insert into TimeTracking ("Peer", "Date", "Time", "State") values ('Lastone', (now())::date,
                                                                   '22:15:33', 2);
insert into TimeTracking ("Peer", "Date", "Time", "State") values ('zdorovenniiyaz5', (now() - interval '1 day')::date,
                                                                   '01:15:33', 1);
insert into TimeTracking ("Peer", "Date", "Time", "State") values ('zdorovenniiyaz5', (now() - interval '1 day')::date,
                                                                   '22:15:33', 2);
insert into TimeTracking ("Peer", "Date", "Time", "State") values ('kapperley2', (now() - interval '2 day')::date,
                                                                   '10:15:33', 1);
insert into TimeTracking ("Peer", "Date", "Time", "State") values ('kapperley2', (now() - interval '2 day')::date,
                                                                   '18:30:33', 2);
insert into TimeTracking ("Peer", "Date", "Time", "State") values ('Amogus', (now() - interval '3 day')::date,
                                                                   '10:15:33', 1);
insert into TimeTracking ("Peer", "Date", "Time", "State") values ('Amogus', (now() - interval '3 day')::date,
                                                                   '18:30:33', 2);
insert into TimeTracking ("Peer", "Date", "Time", "State") values ('kapperley2', (now() - interval '5 day')::date,
                                                                   '00:15:33', 1);
insert into TimeTracking ("Peer", "Date", "Time", "State") values ('kapperley2', (now() - interval '5 day')::date,
                                                                   '00:30:33', 2);
insert into TimeTracking ("Peer", "Date", "Time", "State") values ('kapperley2', (now() - interval '5 day')::date,
                                                                   '10:15:33', 1);
insert into TimeTracking ("Peer", "Date", "Time", "State") values ('kapperley2', (now() - interval '5 day')::date,
                                                                   '20:30:33', 2);
insert into TimeTracking ("Peer", "Date", "Time", "State") values ('yaaz7', (now() - interval '6 day')::date,
                                                                   '10:15:33', 1);
insert into TimeTracking ("Peer", "Date", "Time", "State") values ('yaaz7', (now() - interval '6 day')::date,
                                                                   '18:30:33', 2);
insert into TimeTracking ("Peer", "Date", "Time", "State") values ('yaaz7', (now() - interval '6 day')::date,
                                                                   '19:15:33', 1);
insert into TimeTracking ("Peer", "Date", "Time", "State") values ('yaaz7', (now() - interval '6 day')::date,
                                                                   '20:30:33', 2);