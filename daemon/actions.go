package main

import (
  "context"
  "fmt"
  "log"
  "net"
  "net/http"
  "net/smtp"
  "os"
  "os/exec"
  "strconv"
  "strings"
  "syscall"
  "time"
  "crypto/tls"

  mqtt "github.com/eclipse/paho.mqtt.golang"
  "github.com/godbus/dbus/v5"

  "github.com/emersion/go-imap"
  "github.com/emersion/go-imap/client"
)

type ActionFunc func(runID string, e *Engine, a Action, vars map[string]interface{}, stepID string, flowID string) error

var actionRegistry = map[string]ActionFunc{}

func init() {
  actionRegistry["log"] =          Action_log
  actionRegistry["write_state"] =  Action_write_state
  actionRegistry["write_data"] =   Action_write_data
  actionRegistry["mqtt_publish"] = Action_mqtt_publish
  actionRegistry["dbus_method"] =  Action_dbus_method
  actionRegistry["http"] =         Action_http
  actionRegistry["shell"] =        Action_shell
  actionRegistry["sqlite_query"] = Action_sqlite_query
  actionRegistry["mysql_query"] =  Action_mysql_query
  actionRegistry["smtp"] =         Action_smtp
  actionRegistry["imap_mark_seen"] =  Action_imap_mark_seen
}

func Action_log(runID string, e *Engine, a Action, vars map[string]interface{}, stepID string, flowID string) error {
  msg, ok := vars["message"].(string)
  if !ok {
    msg = a.Message
  }

  msg = expandByTemplate(msg, vars)

  log.Printf("[%s] Action_log - (flow: %s, step: %s): %s", runID, flowID, stepID, msg)

  if e.broadcastChan != nil {
    payload := map[string]interface{}{
      "type":    "log",
      "run_id":  runID,
      "flow_id": flowID,
      "step_id": stepID,
      "message": msg,
    }
    select {
    case e.broadcastChan <- payload:
    default:
      log.Printf("Action_log - Broadcast buffer full, dropping log for run %s", runID)
    }
  }
  return nil
}

func Action_write_state(runID string, e *Engine, a Action, vars map[string]interface{}, stepID string, flowID string) error {
  key, _ := vars["key"].(string)
  val, ok := vars["value"]

  if key == "" || !ok {
    err := fmt.Errorf("missing key or value")
    log.Printf("[%s] Action_write_state - %v", runID, err)
    return err
  }

  if s, ok := val.(string); ok {
    val = expandByTemplate(s, vars)
  }

  prev, _ := e.GetState(key)
  e.WriteState(key, val)
  log.Printf("[%s] Action_write_state - '%s' = %v (was %v)", runID, key, val, prev)
  return nil
}

func Action_write_data(runID string, e *Engine, a Action, vars map[string]interface{}, stepID string, flowID string) error {
  name, ok := vars["_data_record_name"].(string)
  if !ok || name == "" {
    err := fmt.Errorf("missing record name")
    log.Printf("[%s] Action_write_data - %v", runID, err)
    return err
  }

  e.WriteData(name, vars)
  return nil
}

func Action_mqtt_publish(runID string, e *Engine, a Action, vars map[string]interface{}, stepID string, flowID string) error {
  addr := a.Address

  topic, ok := vars["topic"].(string)
  if !ok {
    topic = a.Topic
  }
  payload, ok := vars["payload"].(string)
  if !ok {
    payload = a.Payload
  }
  qos, ok := vars["qos"].(byte)
  if !ok {
    qos = a.Qos
  }
  retained, ok := vars["retained"].(bool)
  if !ok {
    retained = a.Retained
  }

  log.Printf("[%s] Action_mqtt_publish - addr: %s, [%s] %s", runID, addr, topic, payload)

  if addr == "" || topic == "" {
    err := fmt.Errorf("missing address or topic")
    log.Printf("[%s] Action_mqtt_publish - %v", runID, err)
    return err
  }

  address := addr
  if !strings.Contains(address, "://") {
    address = "tcp://" + address
  }

  opts := mqtt.NewClientOptions().AddBroker(address)
  
  if strings.HasPrefix(address, "ssl://") || strings.HasPrefix(address, "tls://") {
    opts.SetTLSConfig(&tls.Config{
      InsecureSkipVerify: a.Insecure,
    })
  }
  
  opts.SetClientID("automagic-pub-" + runID)

  if a.Username != "" {
    opts.SetUsername(a.Username)
    opts.SetPassword(a.Password)
  }

  client := mqtt.NewClient(opts)
  if token := client.Connect(); token.Wait() && token.Error() != nil {
    log.Printf("[%s] Action_mqtt_publish - connection error: %v", runID, token.Error())
    return token.Error()
  }
  defer client.Disconnect(250)

  token := client.Publish(topic, qos, retained, payload)
  token.Wait()

  log.Printf("[%s] Action_mqtt_publish - success [%s] %s", runID, topic, payload)
  return nil
}

func Action_dbus_method(runID string, e *Engine, a Action, vars map[string]interface{}, stepID string, flowID string) error {
  if a.Destination == "" || a.Path == "" || a.Interface == "" || a.Method == "" || a.Address == "" {
    err := fmt.Errorf("missing required dbus params")
    log.Printf("[%s] Action_dbus_method - %v", runID, err)
    return err
  }

  timeout := 2 * time.Second
  if a.Timeout != "" {
    if parsed, err := time.ParseDuration(a.Timeout); err == nil {
      timeout = parsed
    } else {
      log.Printf("[%s] Action_dbus_method - invalid timeout '%s', defaulting to 2s: %v", runID, a.Timeout, err)
    }
  }

  var args []interface{}
  for _, raw := range a.Args {
    argMap, isMap := raw.(map[string]interface{})
    if !isMap {
      log.Printf("[%s] Action_dbus_method - argument must be an object {type, value}", runID)
      continue
    }

    t, _ := argMap["type"].(string)
    val := argMap["value"]

    if s, ok := val.(string); ok {
      val = expandByTemplate(s, vars)
    }

    args = append(args, e.ResolveDBusArg(t, val))
  }

  conn, err := e.connectToDBus(a.Address)
  if err != nil {
    log.Printf("[%s] Action_dbus_method - connect error: %v", runID, err)
    return err
  }
  defer conn.Close()

  obj := conn.Object(a.Destination, dbus.ObjectPath(a.Path))
  ctx, cancel := context.WithTimeout(context.Background(), timeout)
  defer cancel()

  call := obj.CallWithContext(ctx, a.Interface+"."+a.Method, 0, args...)
  if call.Err != nil {
    log.Printf("[%s] Action_dbus_method - call error: %v", runID, call.Err)
    return call.Err
  }

  log.Printf("[%s] Action_dbus_method - success: %s.%s", runID, a.Interface, a.Method)
  return nil
}

func Action_http(runID string, e *Engine, a Action, vars map[string]interface{}, stepID string, flowID string) error {
  url := expandByTemplate(a.Address, vars)

  payload := ""
  if a.Payload != "" {
    payload = expandByTemplate(a.Payload, vars)
  }

  if url == "" {
    err := fmt.Errorf("missing address")
    log.Printf("[%s] Action_http - %v", runID, err)
    return err
  }

  req, err := http.NewRequest(a.Method, url, strings.NewReader(payload))
  if err != nil {
    log.Printf("[%s] Action_http - init error: %v", runID, err)
    return err
  }

  req.Header.Set("User-Agent", "automagicd/1.0")

  if a.ContentType != "" {
    req.Header.Set("Content-Type", a.ContentType)
  }

  if a.Username != "" {
    req.SetBasicAuth(a.Username, a.Password)
  }

  timeout := 10 * time.Second
  if a.Timeout != "" {
    if parsed, err := time.ParseDuration(a.Timeout); err == nil {
      timeout = parsed
    } else {
      log.Printf("[%s] Action_http - invalid timeout '%s', defaulting to 10s: %v", runID, a.Timeout, err)
    }
  }

  customTransport := http.DefaultTransport.(*http.Transport).Clone()
  if a.Insecure {
    if customTransport.TLSClientConfig == nil {
      customTransport.TLSClientConfig = &tls.Config{}
    }
    customTransport.TLSClientConfig.InsecureSkipVerify = true
  }

  client := &http.Client{
    Timeout:   timeout,
    Transport: customTransport,
  }

  resp, err := client.Do(req)
  if err != nil {
    log.Printf("[%s] Action_http - error: %v", runID, err)
    return err
  }
  defer resp.Body.Close()

  log.Printf("[%s] Action_http - payload: %s", runID, payload)
  log.Printf("[%s] Action_http - %s %s -> %d", runID, a.Method, url, resp.StatusCode)

  if resp.StatusCode >= 400 {
    return fmt.Errorf("HTTP error status: %d", resp.StatusCode)
  }

  return nil
}

func Action_shell(runID string, e *Engine, a Action, vars map[string]interface{}, stepID string, flowID string) error {
  commandTemplate := a.Command
  if override, ok := vars["command"].(string); ok && override != "" {
    commandTemplate = override
  }

  if commandTemplate == "" {
    err := fmt.Errorf("no command provided")
    log.Printf("[%s] Action_shell - %v", runID, err)
    return err
  }

  cmdStr := expandByTemplate(commandTemplate, vars)
  cmd := exec.Command("sh", "-c", cmdStr)

  runAsUser := e.SessionUser
  if a.RunAs != "" && (e.SessionUser == nil || a.RunAs != e.SessionUser.Username) {
    privUser, err := e.resolvePrivilegedUser(a.RunAs)
    if err != nil {
      log.Printf("[%s] Action_shell - run_as %q denied: %v", runID, a.RunAs, err)
      return fmt.Errorf("run_as %q denied: %v", a.RunAs, err)
    }
    runAsUser = privUser
  }

  if runAsUser != nil {
    uid, _ := strconv.Atoi(runAsUser.Uid)
    gid, _ := strconv.Atoi(runAsUser.Gid)

    cmd.SysProcAttr = &syscall.SysProcAttr{
      Credential: &syscall.Credential{Uid: uint32(uid), Gid: uint32(gid)},
    }

    cmd.Env = os.Environ()
    cmd.Env = append(cmd.Env,
      "HOME="+runAsUser.HomeDir,
      "USER="+runAsUser.Username,
      "LOGNAME="+runAsUser.Username,
      fmt.Sprintf("DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/%d/dbus/user_bus_socket", uid),
      "PATH=/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin",
    )
  }

  output, err := cmd.CombinedOutput()
  if err != nil {
    log.Printf("[%s] Action_shell - error: %v, output: %s", runID, err, string(output))
    return err
  }

  if len(output) > 0 {
    log.Printf("[%s] Action_shell - output: %s", runID, strings.TrimSpace(string(output)))
  }
  return nil
}

func Action_sqlite_query(runID string, e *Engine, a Action, vars map[string]interface{}, stepID string, flowID string) error {
  dbPath := expandByTemplate(a.Path, vars)
  if dbPath == "" {
    dbPath = expandByTemplate(a.Address, vars)
  }

  query := expandByTemplate(a.Query, vars)
  if query == "" {
    query = expandByTemplate(a.Payload, vars)
  }

  if dbPath == "" || query == "" {
    err := fmt.Errorf("missing params")
    log.Printf("[%s] Action_sqlite_query - %v", runID, err)
    return err
  }

  db, err := e.getSQLConnection("sqlite", "", "", "", dbPath, "")
  if err != nil {
    log.Printf("[%s] Action_sqlite_query - connect error: %v", runID, err)
    return err
  }

  _, err = db.Exec(query)
  if err != nil {
    log.Printf("[%s] Action_sqlite_query - exec error: %v | Query: %s", runID, err, query)
    return err
  }

  log.Printf("[%s] Action_sqlite_query - success", runID)
  return nil
}

func Action_mysql_query(runID string, e *Engine, a Action, vars map[string]interface{}, stepID string, flowID string) error {
  address := expandByTemplate(a.Address, vars)
  user := expandByTemplate(a.Username, vars)
  pass := expandByTemplate(a.Password, vars)
  dbPath := expandByTemplate(a.Path, vars)
  dbname := expandByTemplate(a.Database, vars)
  query := expandByTemplate(a.Query, vars)

  if dbname == "" || query == "" || address == "" {
    err := fmt.Errorf("missing params")
    log.Printf("[%s] Action_mysql_query - %v", runID, err)
    return err
  }

  db, err := e.getSQLConnection("mysql", address, user, pass, dbPath, dbname)
  if err != nil {
    log.Printf("[%s] Action_mysql_query - connect error: %v", runID, err)
    return err
  }

  _, err = db.Exec(query)
  if err != nil {
    log.Printf("[%s] Action_mysql_query - exec error: %v | Query: %s", runID, err, query)
    return err
  }

  log.Printf("[%s] Action_mysql_query - success", runID)
  return nil
}

type insecureAuth struct {
  username, password string
}

func (a *insecureAuth) Start(server *smtp.ServerInfo) (string, []byte, error) {
  resp := []byte("\x00" + a.username + "\x00" + a.password)
  return "PLAIN", resp, nil
}

func (a *insecureAuth) Next(fromServer []byte, more bool) ([]byte, error) {
  if more {
    return nil, fmt.Errorf("unexpected server challenge")
  }
  return nil, nil
}

func Action_smtp(runID string, e *Engine, a Action, vars map[string]interface{}, stepID string, flowID string) error {
  address := expandByTemplate(a.Address, vars)
  user := expandByTemplate(a.Username, vars)
  pass := expandByTemplate(a.Password, vars)

  plucking := func(key, fallback string) string {
    if v, ok := vars[key].(string); ok && v != "" {
      return expandByTemplate(v, vars)
    }
    return expandByTemplate(fallback, vars)
  }

  to := plucking("to", a.To) 
  bcc := plucking("bcc", a.Bcc)
  from := plucking("from", a.From)
  subject := plucking("subject", a.Subject)
  body := plucking("body", a.Body)

  if address == "" || to == "" || from == "" {
    err := fmt.Errorf("missing required params (address: %s, to: %s, from: %s)", address, to, from)
    log.Printf("[%s] Action_smtp - %v", runID, err)
    return err
  }

  host, port, err := net.SplitHostPort(address)
  if err != nil {
    host = address
    port = "587"
    address = net.JoinHostPort(host, port)
  }

  var recipients []string
  for _, t := range strings.Split(to+ "," + bcc, ",") {
    clean := strings.TrimSpace(t)
    if clean != "" {
      recipients = append(recipients, clean)
    }
  }

  msgDate := ""
  if d, ok := vars["date"].(string); ok && d != "" {
    msgDate = d
  } else {
    msgDate = time.Now().Format(time.RFC1123Z)
  }

  msg := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\nDate: %s\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=\"utf-8\"\r\n\r\n%s", 
    from, to, subject, msgDate, body)

  var auth smtp.Auth
  if user != "" && pass != "" {
    auth = &insecureAuth{username: user, password: pass}
  }

  tlsConfig := &tls.Config{
    ServerName:         host,
    InsecureSkipVerify: a.Insecure, 
  }

  var c *smtp.Client

  if port == "465" {
    conn, tlsErr := tls.Dial("tcp", address, tlsConfig)
    if tlsErr != nil {
      log.Printf("[%s] Action_smtp - TLS dial error: %v", runID, tlsErr)
      return tlsErr
    }
    c, err = smtp.NewClient(conn, host)
  } else {
    c, err = smtp.Dial(address)
    if err == nil {
      if ok, _ := c.Extension("STARTTLS"); ok {
        err = c.StartTLS(tlsConfig)
      }
    }
  }

  if err != nil {
    log.Printf("[%s] Action_smtp - connection error: %v", runID, err)
    return err
  }
  defer c.Close()

  if auth != nil {
    if err = c.Auth(auth); err != nil {
      log.Printf("[%s] Action_smtp - auth error: %v", runID, err)
      return err
    }
  }

  if err = c.Mail(from); err != nil {
    return err
  }
  for _, r := range recipients {
    if err = c.Rcpt(r); err != nil {
      return err
    }
  }

  w, err := c.Data()
  if err != nil {
    return err
  }
  _, err = w.Write([]byte(msg))
  w.Close()
  c.Quit()

  log.Printf("[%s] Action_smtp - success: email sent to %d recipients", runID, len(recipients))
  return nil
}

func Action_imap_mark_seen(runID string, e *Engine, a Action, vars map[string]interface{}, stepID string, flowID string) error {
  address := expandByTemplate(a.Address, vars)
  user := expandByTemplate(a.Username, vars)
  pass := expandByTemplate(a.Password, vars)
  
  mailbox := expandByTemplate(a.Path, vars)
  if mailbox == "" {
    mailbox = "INBOX"
  }

  var uidRaw string
  if val, ok := vars["uid"]; ok && val != nil && val != "" {
    uidRaw = fmt.Sprintf("%v", val)
  }
  if uidRaw == "" {
    uidRaw = expandByTemplate(a.Payload, vars)
  }

  uid, err := strconv.ParseUint(uidRaw, 10, 32)
  if err != nil || uid == 0 {
    err = fmt.Errorf("invalid or missing UID: %s", uidRaw)
    log.Printf("[%s] Action_imap_mark_seen - %v", runID, err)
    return err
  }

  if address == "" || user == "" || pass == "" {
    return fmt.Errorf("missing IMAP connection params")
  }

  host, port, _ := net.SplitHostPort(address)
  if port == "" { 
    host = address
    port = "993"
    address = net.JoinHostPort(host, port) 
  }

  tlsConfig := &tls.Config{
    ServerName:         host,
    InsecureSkipVerify: a.Insecure,
  }
  
  var c *client.Client
  if port == "993" {
    c, err = client.DialTLS(address, tlsConfig)
  } else {
    c, err = client.Dial(address)
    if err == nil {
      if ok, _ := c.SupportStartTLS(); ok { 
        err = c.StartTLS(tlsConfig) 
      }
    }
  }

  if err != nil { 
    return err 
  }
  defer c.Logout()

  if err := c.Login(user, pass); err != nil { 
    return err 
  }
  if _, err := c.Select(mailbox, false); err != nil { 
    return err 
  }

  seqset := new(imap.SeqSet)
  seqset.AddNum(uint32(uid))
  item := imap.FormatFlagsOp(imap.AddFlags, true)
  flags := []interface{}{imap.SeenFlag}
  
  if err := c.UidStore(seqset, item, flags, nil); err != nil {
    log.Printf("[%s] Action_imap_mark_seen - store error: %v", runID, err)
    return err
  }

  log.Printf("[%s] Action_imap_mark_seen - success: UID %d marked as read", runID, uid)
  return nil
}
