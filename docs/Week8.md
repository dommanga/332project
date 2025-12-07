# **Week 8 Progress (Dec 1 – Dec 7, 2025)**

## **This Week’s Progress**

### **1. Fat JAR Build + Deployment Pipeline 정식 확립 (Jimin)**

#### 📦 **dist-sort.jar 성공 생성 & 실행 환경 표준화**

* `sbt-assembly` 기반으로 **master/worker 공용 실행 가능한 단일 JAR**(`dist-sort.jar`) 완성
* Netty/gRPC 서비스 로더 deduplication 문제 해결

  * `META-INF/services` 항목 concat 처리
  * Netty version properties conflict 해결
* Master/Worker 모두 아래처럼 실행 가능하게 통일:

  ```bash
  java -Xms2G -Xmx4G -jar dist-sort.jar master <N>
  java -Xms2G -Xmx4G -jar dist-sort.jar worker <master_ip:port> -I <input> -O <output>
  ```

#### 🚀 **배포 방식 확정: SBT → JAR로 전환**

* 기존 “각 worker에서 sbt compile” 방식 제거
* Worker들은 오직:

  * git pull →
  * 최신 `dist-sort.jar` 이용
* 모든 노드에서 실행 로직이 **표준화**되어 운영 안정성 향상

---

### **2. deploy.sh 리팩터링 & 자동화 강화**

* `deploy.sh`가 Week 8 기준 **공식 실행 도구**로 변경

  * `init`, `update`, `start`, `restart`, `check` 모두 jar 기반으로 동작
* Worker 실행:

  ```bash
  nohup java $JAVA_OPTS -jar dist-sort.jar worker ...
  ```

  형태로 안정적 백그라운드 실행
* Fault injection, restart scenario, logs 등 전체 실험 루틴 자동화 완료
* 특히 `start_workers()`와 `restart_worker()`가 JAR 기반으로 완전히 정리됨

---

### **3. Worker Rejoin + Recovery Mode 정식 구현**

#### 🔄 **Worker crashes → rejoin → resume pipeline 정상 작동**

* Worker가 mid-shuffle에서 죽어도:

  1. Master의 heartbeat prune thread에서 사망 감지
  2. orphaned partition owner 재등록
  3. Worker 재시작 시 **동일 worker ID로 재조인**
  4. Master가 해당 worker에게 PartitionPlan 재전송
* Worker는 재시작 시 `sent-checkpoint/` 를 감지해:

  * sampling / sort / partition / shuffle 단계 **skip**
  * finalize + recovery 단계만 수행
* FT shuffle/recovery 파이프라인 완성

#### ✔ 상태 기반 로그 정리

* "Worker X DEAD"
* "Worker X rejoined"
* "Resent PartitionPlan to Worker X"
* "Recovery mode: waiting for finalize"
  등 전체 로깅 흐름을 Week 8 버전으로 정제

---

### **4. Master Finalize-Orchestration + Recovery Trigger 개선**

* Master에서 worker 재조인 시 **모든 worker의 shuffle 종료 여부** 재평가
* 마지막 rejoined worker까지 shuffle 완료되면:

  * `triggerFinalizePhase()` 자동 호출
  * 모든 worker의 finalize 진행 확인
* Finalize 중 crash가 발생해도 재조인 시 다시 finalize 명령을 재전달하도록 안전장치 추가
* Dead worker 발생 시 merge 단계로 진입하지 않도록 merge 조건 정리

---

### **5. Integration Test (3–5 workers) 성공 (Team)**

실제 VM 환경(2.2.2.xxx)에서 테스트:

#### 🧪 **Case 1: No failure**

* 5 workers 정상 실행
* shuffle → finalize → write outputs 모두 정상
* `deploy.sh check` 결과 input/output record count 일치

#### 🧪 **Case 2: Worker crash**

* Worker 1 강제 종료
* Master에서 바로 감지
* Worker 1 재조인
* Recovery mode로 finalize만 수행하거나, 처음부터 정상 수행
* 최종 결과 record count + key ordering 정상 확인

#### 🧪 **Case 3: Worker crash - Restart during finalize**

* Worker finalize 직전에 kill
* 재시작 시 finalize 단계부터 재수행
* partition consistency 유지 확인

---


## **Key Decisions**

* **sbt-run → jar-run** 방식으로 팀 전체 실행 환경 통일
* Worker recovery는 **sent-checkpoint를 기준으로 deterministic하게 재실행**
* Fault injection 실험은 deploy.sh를 통해 관리
* Worker 재조인 시 필요한 메시지(PartitionPlan, finalize cmd)는 **master가 targeted 전송**
