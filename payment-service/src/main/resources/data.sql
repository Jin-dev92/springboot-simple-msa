-- 인메모리 DB 이므로 기동할 때마다 이 초기 데이터로 시작한다.
-- userId 는 auth-service 가 user, admin 순으로 저장하고 id 가 자동 증가하므로 1, 2 다.
insert into account (user_id, balance) values (1, 1000000);
insert into account (user_id, balance) values (2, 1000000);
