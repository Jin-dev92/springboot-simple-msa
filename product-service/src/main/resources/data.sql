-- 인메모리 DB 이므로 기동할 때마다 이 초기 데이터로 시작한다.
-- version 은 변경 순번이다. 구독자가 이벤트를 놓쳤는지 판단하는 근거이므로
-- 초기 데이터도 반드시 채워야 한다(1 부터).
insert into product (name, price, stock, version) values ('키보드', 89000, 30, 1);
insert into product (name, price, stock, version) values ('모니터', 320000, 12, 1);
insert into product (name, price, stock, version) values ('마우스', 45000, 50, 1);
