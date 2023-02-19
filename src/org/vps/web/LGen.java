package org.vps.web;

import java.awt.*;
import java.awt.event.*;
import java.io.*;
import java.net.URI;
import java.sql.*;
import java.util.*;

import org.vps.web.DBConn;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.traversal.*;


/**
 * @author Pawel S. Veselov
 */
public class LGen {

    final static int AUTH_PASS = 1;
    final static int AUTH_SUB = 2;

    final static String VERSION = "1.13";
    final static String APP = "Load Gen, for 4.0";

    static Properties config;

    static DBConn dbUrl;

    static int max_threads;
    static int active_threads;
    static int requestsPerUser;
    static String password;
    static String baseURL;
    static String uniqueHeader;

    static long runTime;

    static boolean allStop;

    static int greenLimit;
    static int redLimit;
    static int awtPeriod;
    static int singleHeight;
    static int threadSleep;
    static boolean keepPace;
    static int minThreadSleep;
    static int maxWidth;
    static int errorLinger;
    static int sleepError;
    static int authMethod;
    static boolean useCache;
    static String cacheFileName;
    static int cdsVersion;
    static int thread_wakeup_interval;

    static int nodeReload;
    static int leafReload;
    static boolean scrollPages;
    static float chanceViewItem;
    static float chancePurchaseItem;
    static float chanceViewEveryItem;

    static boolean allowChangeLanguage;
    static boolean allowSetLanguage;

    static boolean onWindows = File.separatorChar == '\\';

    static Win window;
    static ThrottleWin throttleWindow;
    static PageStatWin pageStatWindow;

    static Connection dbConn;
    static ResultSet allSubRS;
    static Iterator vecSubIterator;
    static Statement stmt;

    static PreparedStatement planStmt;

    static Random random = new Random(System.currentTimeMillis());

    static HashMap categoryLists ;
    final static int CAT_NODE = 1;
    final static int CAT_LEAF = 2;
    static Vector badDevices = new Vector();

    static Vector subscribers;
    static int subscriberSource;

    // prepared statement to use to load subscriber info
    static PreparedStatement loadSubById;
    static PreparedStatement loadSubByLogin;

    static Object paintLock = new Object();

    static boolean thread_monitor[];

    static HashMap planCategoryMap = new HashMap();

    final static String LOAD_SUB_ID_QUERY = "select subscriber_key, unique_device_id, handset_id, subscriber_id from subscriber where subscriber_id = ?";
    final static String LOAD_SUB_LOGIN_QUERY = "select subscriber_key, unique_device_id, handset_id, subscriber_id from subscriber where subscriber_key = ?";
    final static String LOAD_ALL_SUBS_QUERY = "select subscriber_id from subscriber";
    final static String PLAN_QUERY = "select category_plan_id from subscriber_plan_map where subscriber_id = ?";

    // how much, in %, should the buffer between maximum response time
    // and the scale maximum be.
    final static double scaleBuffer = 0.8D;

    // how much should the minimum scale be.
    final static int minScale = 200;

    // how much, in % should the response time go down to,
    // before we rescale
    final static double scaleTolerance = 0.6D;

    // scale width. Don't change, really.
    final static int scaleWidth = 50;

    // how wide is the diagramm bar (in pixels)
    final static int barWidth = 40;

    // what's the gap between diagramm bars
    final static int barGap = 10;

    final static String AC_RST = "rst";
    final static String AC_QUIT = "quit";
    final static String AC_THROTTLE = "throttle";
    final static String AC_APPLY_TV = "apply_tv";
    final static String AC_RESET_TV = "reset_tv";
    final static String AC_CLOSE_TW = "close_tw";
    final static String AC_PAGE_STAT = "page_stat";
    final static String AC_CLOSE_PSW = "close_psw";

    final static int BKT_LOGIN = 0;  // login
    final static int BKT_CAT_LIST = 1; // category list
    final static int BKT_CON_LIST = 2; // item list (category view)
    final static int BKT_DETAILS = 3; // item details page
    final static int BKT_PUR_CF = 4; // purchase terms & conditions page
    final static int BKT_PUR = 5;   // actual purchase
    final static int BKT_DD = 6;    // descriptor download
    final static int BKT_DL = 7;    // content (binary) download
    final static int BKT_FRONT = 8; // front page displayed

    final static String [] bucketNames = {
        "login",
        "category list",
        "item list",
        "item details",
        "T&C",
        "purchase",
        "descriptor dl",
        "binary dl",
        "main page" };

    final static int PSW_LEFT_INSET = 5;
    final static int PSW_RIGHT_INSET = 6;
    final static int PSW_WIDTH = 100;
    final static int PSW_TOP_INSET = 3;
    final static int PSW_BOTTOM_INSET = 3;
    final static int PSW_LINE_INTERVAL = 3;
    final static int PSW_SPACE = 10;

    final static int SUBSRC_DB = 1;
    final static int SUBSRC_FILE = 2;

    final static int SUBSRC_TYPE_ID = 1;
    final static int SUBSRC_TYPE_LOGIN = 2;

    public static void main(String a[]) {
        try {
            main0(a);
        } catch (Throwable t) {
            Log.error("Exception thrown in main thread", t);
        }
    }

    private static void main0(String a[]) throws Exception {

        Log.info("-== "+APP+" ==-");
        Log.info("-== version "+VERSION+" ==-");

        String props = "gen.properties";

        // Log.setLogLevel(Log.NOISE);

        if (a.length > 0) {
            props = a[0];
        }

        Log.info("Loading properties file from "+props);

        FileInputStream fr = new FileInputStream(props);
        config = new Properties();
        config.load(fr);
        fr.close();

        loadProperties();

        if (!useCache) {
            Log.warn("Cacheing is not turned on. Expect slow loads every time lgen is started");
        }

        Class.forName("oracle.jdbc.driver.OracleDriver");

        dbConn = DriverManager.getConnection(dbUrl.url,
                dbUrl.user, dbUrl.password);

        baseURL = config.getProperty("url");
        if (baseURL == null) {
            throw new RuntimeException("'url' property is NULL");
        }
        if (!baseURL.endsWith("/")) {
            baseURL += '/';
        }

        loadDatabase();
        startLoad();

        pokeThreads();
    }

    static void pokeThreads() {

        while (true) {

            try {
                Thread.sleep(thread_wakeup_interval);
            } catch (Exception e) {
            }

            if (allStop) { return; }

            synchronized (thread_monitor) {

                for (int i=0; i<Math.min(active_threads,
                            thread_monitor.length); i++) {

                    if (!thread_monitor[i]) {
                        Log.info("Thread "+i+" had to be restarted");
                        (new Exec(i)).start();
                        break;
                    }
                }
            }
        }
    }

    static void saveCache() throws Exception {

        if (!useCache) {
            return;
        }

        File file = new File(cacheFileName);
        if ((file.exists() && !file.canWrite()) || !file.getParentFile().canWrite()) {
            Log.error("Can't open cache file '"+cacheFileName+"' for writing");
            return;
        }

        FileOutputStream fos = new FileOutputStream(file);
        ObjectOutputStream oos = new ObjectOutputStream(fos);

        oos.writeObject(VERSION);
        oos.writeObject(categoryLists);
        oos.writeObject(badDevices);

        oos.close();
        fos.close();

        Log.info("Saved cache file "+cacheFileName);
    }

    static void loadCache() throws Exception {

        // try to load cache first.
        if (!useCache) { return; }

        File cacheFile = new File(cacheFileName);
        if (!cacheFile.exists()) {
            Log.info("Cache file '"+cacheFileName+
                    "' does not exist, will build");
            return;
        }

        if (!cacheFile.canRead()) {
            Log.error("Cache file "+cacheFile+" is not readable");
            return;
        }

        long cacheModified = cacheFile.lastModified();

        Statement s = dbConn.createStatement();

        try {

            ResultSet rs = s.executeQuery("select max(mod_date) from handset");
            if (!rs.next()) { return; }
            if (rs.getTimestamp(1).getTime() > cacheModified) {
                Log.info("HANDSET table modified, will rebuild cache");
                return;
            }

            rs = s.executeQuery("select max(mod_date) from category_item_map");
            if (!rs.next()) { return; }
            if (rs.getTimestamp(1).getTime() > cacheModified) {
                Log.info("CATEGORY_ITEM_MAP table modified, will rebuild cache");
                return;
            }

            if (cdsVersion == 4) {
                rs = s.executeQuery("select max(mod_date) from handset_locale_edition_map");
            } else {
                rs = s.executeQuery("select max(mod_date) from handset_content_map");
            }

            if (!rs.next()) { return; }
            if (rs.getTimestamp(1).getTime() > cacheModified) {
                Log.info("HANDSET_LOCALE_EDITION_MAP table modified, will rebuild cache");
                return;
            }

            ObjectInputStream dis =
                new ObjectInputStream(new FileInputStream(cacheFile));

            HashMap categoryLists;
            Vector badDevices;

            try {
                String version = (String)dis.readObject();
                if (!version.equals(VERSION)) {
                    Log.warn("Cache file is of version "+version+
                            ", and is outdated. Will rebuild");
                    return;
                }
                categoryLists = (HashMap)dis.readObject();
                badDevices = (Vector)dis.readObject();
            } catch (Throwable e) {
                Log.warn("Cache file corrupted. Will rebuild", e);
                dis.close();
                cacheFile.delete();
                return;
            }

            rs = s.executeQuery("select unique(handset_id) from subscriber where handset_id is not null");
            Vector allDevices = new Vector();

            while (rs.next()) {
                allDevices.add(new Long(rs.getLong(1)));
            }

            while (allDevices.size() > 0) {

                Long next = (Long)allDevices.elementAt(0);
                if (!badDevices.contains(next) &&
                        !categoryLists.containsKey(next)) {
                    Log.info("Device "+next+" added into subscriber database, will rebuild cache");
                    return;
                }
                allDevices.removeElementAt(0);
            }

            LGen.categoryLists = categoryLists;
            LGen.badDevices = badDevices;

        } finally {
            s.close();
        }

        Log.info("Loaded cache, "+categoryLists.size()+" devices and "+badDevices.size()+" bad devices");

        for (Iterator i = categoryLists.keySet().iterator(); i.hasNext(); ) {
            Long next = (Long)i.next();
            DevInfo dev = (DevInfo)categoryLists.get(next);
            Log.info("Device "+next+" has "+dev.leafCategories.length+
                    " leaf and "+dev.nodeCategories.length+
                    " node categories");
        }

    }

    static void loadPlanCategoryMap() throws Exception {

        Statement s = dbConn.createStatement();
        ResultSet rs = s.executeQuery("select vend_category_id, category_plan_id from plan_vend_category_map");

        while (rs.next()) {

            Long planId = new Long(rs.getLong(2));
            Long categoryId = new Long(rs.getLong(1));

            Vector categories = (Vector)planCategoryMap.get(planId);
            if (categories == null) {
                categories = new Vector();
                planCategoryMap.put(planId, categories);
            }

            categories.add(categoryId);
        }

        s.close();

        for (Iterator i = planCategoryMap.keySet().iterator(); i.hasNext(); ) {
            Long planId = (Long)i.next();
            Vector catList = (Vector)planCategoryMap.get(planId);
            Log.debug("Loaded "+catList.size()+" categories for plan "+planId);
        }
    }

    static boolean isCategoryAllowed(long id) {

        Long cat = new Long(id);
        for (Iterator i = planCategoryMap.values().iterator(); i.hasNext(); ) {

            Vector v = (Vector)i.next();
            if (v.contains(cat)) {
                return true;
            }
        }

        return false;
    }

    static void loadDatabase() throws Exception {

        loadPlanCategoryMap();

        loadCache();
        if (categoryLists != null) { return; }

        categoryLists = new HashMap();

        Statement s = dbConn.createStatement();

        ResultSet rs = s.executeQuery("select unique(handset_id) from subscriber where handset_id is not null");

        Vector deviceVector = new Vector();

        while (rs.next()) {
            deviceVector.add(new Long(rs.getLong(1)));
        }
        if (deviceVector.size() == 0) {
            Log.error("Failed to load device list off of the subscriber database");
            System.exit(1);
        }

        Log.info("Found "+deviceVector.size()+" unique devices");

        PreparedStatement ps = dbConn.prepareStatement("select parent_category_id from vend_category where vend_category_id = ?");

        for (Iterator i = deviceVector.iterator(); i.hasNext(); ) {

            Long deviceId = (Long)i.next();
            DevInfo devInfo = new DevInfo();

            if (cdsVersion == 4) {

                rs = s.executeQuery("select vend_category_id, count(category_item_id) from category_item_map where category_item_id in (\n" +
                        "select category_item_id from category_item where CATEGORY_ITEM_STATUS_ID = 1 intersect\n" +
                        "select unique category_item_id from handset_locale_edition_map where handset_id = "+deviceId+") group by vend_category_id");
            } else {

                rs = s.executeQuery("select vend_category_id, count(category_item_id) from category_item_map where category_item_id in (\n" +
                        "select category_item_id from category_item where CATEGORY_ITEM_STATUS_ID = 1 intersect\n" +
                        "select unique category_item_id from handset_content_map where handset_id = "+deviceId+") group by vend_category_id");

            }

            Vector leafVector = new Vector();

            while (rs.next()) {
                long cid = rs.getLong(1);
                if (!isCategoryAllowed(cid)) {
                    Log.debug("Skipping category "+cid+", as it doesn't belong to any plans");
                    continue;
                }

                leafVector.add(new CategoryInfo(cid, rs.getInt(2)));
            }

            if (leafVector.size() == 0) {
                Log.error("Found NO leaf categories for device "+deviceId);
                badDevices.add(deviceId);
                continue;
            }

            Vector nodeVector = new Vector();

            for (Iterator j = leafVector.iterator(); j.hasNext(); ) {

                CategoryInfo currentLeaf = (CategoryInfo)j.next();

                do {

                    ps.setLong(1, currentLeaf.categoryId);
                    ResultSet crs = ps.executeQuery();
                    if (!crs.next()) {
                        Log.warn("Failed to load category parent for category "+
                                currentLeaf.categoryId);
                        break;
                    } else {
                        long cid = crs.getLong(1);
                        if (crs.next()) {
                            Log.warn("Over 1 parent returned for category "+
                                    currentLeaf.categoryId);
                        }

                        CategoryInfo next = new CategoryInfo(cid);
                        int aux = nodeVector.indexOf(next);
                        if (aux >= 0) {
                            next = (CategoryInfo)nodeVector.elementAt(aux);
                        } else {
                            nodeVector.add(next);
                        }

                        next.count += currentLeaf.count;

                        currentLeaf.parent = next;
                        next.children.add(currentLeaf);

                        currentLeaf = next;
                    }

                } while (currentLeaf.categoryId != 1L);
            }

            devInfo.leafCategories =
                    (CategoryInfo[])leafVector.toArray(new CategoryInfo[0]);
            devInfo.nodeCategories =
                    (CategoryInfo[])nodeVector.toArray(new CategoryInfo[0]);

            if (devInfo.nodeCategories.length == 0) {
                Log.error("Found NO node categories for device "+deviceId);
                continue;
            }

            Log.info("Loaded "+devInfo.leafCategories.length+" leaf, "+
                    devInfo.nodeCategories.length+
                    " node categories for device "+deviceId);

            categoryLists.put(deviceId, devInfo);
        }

        ps.close();
        s.close();

        saveCache();
    }

    private static SubInfo retrieveNextSub() throws Exception {

        if (subscriberSource == SUBSRC_FILE) {

            if (vecSubIterator == null ||
                    !vecSubIterator.hasNext()) {
                vecSubIterator = subscribers.iterator();
            }

            return (SubInfo)vecSubIterator.next();

        } else if (subscriberSource == SUBSRC_DB) {
            if (allSubRS == null || !allSubRS.next()) {
                allSubRS = stmt.executeQuery(LOAD_ALL_SUBS_QUERY);
                if (!allSubRS.next()) {
                    Log.error("No subscribers can be loaded");
                    System.exit(1);
                }
            }
            return new SubInfo(allSubRS.getLong(1));
        } else {
            throw new RuntimeException("Unknown subscriber source "+
                    subscriberSource);
        }
    }

    static synchronized SubInfo getNextSub() throws Exception {

        while (true) {

            SubInfo si = retrieveNextSub();
            if (!si.load()) { continue; }

            Long devId = new Long(si.deviceId);

            if (badDevices.contains(devId)) {
                Log.warn("Skipping subscriber "+si.login+", bad device");
                continue;
            }

            if (planStmt == null) {
                planStmt = dbConn.prepareStatement(PLAN_QUERY);
            }

            planStmt.setLong(1, si.id);
            ResultSet rs = planStmt.executeQuery();
            while (rs.next()) {
                long planId = rs.getLong(1);
                Vector planCategories =
                        (Vector)planCategoryMap.get(new Long(planId));
                si.allowedCategories.addAll(planCategories);
            }

            rs.close();

            if (si.allowedCategories.size() == 0) {
                Log.warn("Skipping subscriber "+si.login+
                        ", subscriber has no allowed categories (no plans?)");
                continue;
            }

            DevInfo di = (DevInfo)categoryLists.get(devId);

            for (int i=0; i<di.leafCategories.length; i++) {

                CategoryInfo ci = di.leafCategories[i];
                Long cid = new Long(ci.categoryId);
                if (!si.allowedCategories.contains(cid)) {
                    continue;
                }

                si.leafCategories.add(ci);
                while (ci.parent != null) {
                    CategoryInfo ci2 = new CategoryInfo(ci.parent);
                    int aux = si.nodeCategories.indexOf(ci2);
                    if (aux < 0) {
                        si.nodeCategories.add(ci2);
                    } else {
                        ci2 = (CategoryInfo)si.nodeCategories.elementAt(aux);
                    }
                    ci2.count += ci.count;

                    ci = ci2;

                    si.nodeCategories.add(ci);
                }
            }
            return si;
        }
    }

    static float getDoubleF(String s) throws Exception {

        int slIdx = s.indexOf('/');
        if (slIdx < 1) {
            throw new Exception("'/' symbol not found or found first");
        }
        int num;
        int denom;
        String aux = null;
        try {
            num = Integer.parseInt(aux = s.substring(0, slIdx).trim());
        } catch (Exception e) {
            throw new Exception("Failed to parse numerator : " + aux, e);
        }
        try {
            denom = Integer.parseInt(aux = s.substring(slIdx+1).trim());
        } catch (Exception e) {
            throw new Exception("Failed to parse denominator : " + aux, e);
        }

        if (num == 0) { return 0.0F; }
        try {
            return ((float)num)  /  ((float)denom);
        } catch (Exception e) {
            throw new Exception("Failed to translate into number : "+s, e);
        }
    }

    static void loadProperties() throws Exception {

        String url = config.getProperty("dbaccess");
        if (url == null) {
            throw new RuntimeException("No 'dbaccess' property");
        }
        dbUrl = new DBConn(url);

        try {
            max_threads = Integer.parseInt(config.getProperty("max_threads"));
            if (max_threads < 1) {
                throw new Exception("Negative or zero");
            }
        } catch (Exception e) {
            throw new RuntimeException("Property 'max_threads' is missing or invalid");
        }

        thread_monitor = new boolean[max_threads];

        try {
            active_threads =
                Integer.parseInt(config.getProperty("active_threads"));
            if (active_threads < 1) {
                active_threads = max_threads;
            }
        } catch (Exception e) {
            throw new RuntimeException("Property 'active_threads' is missing or invalid");
        }

        try {
            thread_wakeup_interval =
                Integer.parseInt(config.getProperty("thread_wakeup_interval",
                            "1500"));
        } catch (Exception e) {
            thread_wakeup_interval = 1500;
            Log.warn("Invalid property value for 'thread_wakeup_interval', defaulting to 1500");
        }

        try {
            requestsPerUser =
                Integer.parseInt(config.getProperty("requests_per_user"));
            if (requestsPerUser < 1) {
                throw new Exception("Negative or zero");
            }
        } catch (Exception e) {
            throw new RuntimeException("Property 'requests_per_user' is missing or invalid");
        }

        try {
            greenLimit =
                Integer.parseInt(config.getProperty("green_limit", "1000"));
        } catch (Exception e) {
            greenLimit = 1000;
            Log.warn("Invalid property value for 'green_limit', defaulting to 1000", e);
        }

        try {
            redLimit =
                Integer.parseInt(config.getProperty("red_limit", "20000"));
        } catch (Exception e) {
            redLimit = 20000;
            Log.warn("Invalid property value for 'red_limit', defaulting to 20000", e);
        }

        try {
            awtPeriod =
                Integer.parseInt(config.getProperty("awt_update_period",
                            "250"));

        } catch (Exception e) {
            awtPeriod = 250;
            Log.warn("Invalid property value for 'awt_update_period', defaulting to 250", e);
        }

        try {
            threadSleep =
                Integer.parseInt(config.getProperty("thread_sleep", "1000"));
        } catch (Exception e) {
            threadSleep = 1000;
            Log.warn("Invalid property value for 'thread_sleep', defaulting to 1000", e);
        }

        try {
            singleHeight =
                Integer.parseInt(config.getProperty("bar_height", "500"));
        } catch (Exception e) {
            singleHeight = 500;
            Log.warn("Invalid property value for 'bar_height', defaulting to 500", e);
        }

        try {
            minThreadSleep =
                Integer.parseInt(config.getProperty("thread_min_sleep", "100"));
        } catch (Exception e) {
            minThreadSleep = 100;
            Log.warn("Invalid property value for 'thread_min_sleep', defaulting to 100", e);
        }

        try {
            maxWidth =
                Integer.parseInt(config.getProperty("max_width", "0"));
        } catch (Exception e) {
            maxWidth = 0;
            Log.warn("Invalid property value for 'max_width', defaulting to 0", e);
        }

        try {
            errorLinger =
                Integer.parseInt(config.getProperty("error_linger", "2000"));
        } catch (Exception e) {
            errorLinger = 2000;
            Log.warn("Invalid property value for 'error_linger', defaulting to 2000", e);
        }

        try {
            sleepError =
                Integer.parseInt(config.getProperty("sleep_after_error",
                            "5000"));
        } catch (Exception e) {
            sleepError = 5000;
            Log.warn("Invalid property value for 'sleep_after_error', defaulting to 5000", e);
        }

        try {
            nodeReload =
                Integer.parseInt(config.getProperty("hit_count_node", "3"));
            if (nodeReload < 0) {
                throw new Exception("assertion : property hit_count_node < 0");
            }
        } catch (Exception e) {
            nodeReload = 3;
            Log.warn("Invalid property value for 'hit_count_node', defaulting to 3", e);
        }

        try {
            cdsVersion =
                Integer.parseInt(config.getProperty("cds_version", "4"));
            if (cdsVersion != 4 && cdsVersion != 5) {
                throw new Exception("assertion: cds_version must be '4' or '5'");
            }
        } catch (Exception e) {
            cdsVersion = 4;
            Log.warn("Invalid property value for 'cds_version', defaulting to 4", e);
        }

        try {
            chanceViewItem =
                getDoubleF(config.getProperty("view_item_chance", "1/3"));
        } catch (Exception e) {
            chanceViewItem = 0.33F;
            Log.warn("Invalid property value for 'view_item_chance', defaulting to 1/3", e);
        }

        try {
            chancePurchaseItem =
                getDoubleF(config.getProperty("purchase_item_chance",
                            "0/0"));
        } catch (Exception e) {
            chancePurchaseItem = 0.0F;
            Log.warn("Invalid property value for 'purchase_item_chance', defaulting to 0/0", e);
        }

        try {
            chanceViewEveryItem =
                getDoubleF(config.getProperty("view_every_item_chance", "1/5"));
        } catch (Exception e) {
            chanceViewEveryItem = 0.2F;
            Log.warn("Invalid property value for 'view_every_item_chance', defaulting to 1/5");
        }


        Log.info("CDS version code in use : "+cdsVersion);

        try {
            leafReload =
                Integer.parseInt(config.getProperty("hit_count_leaf", "1"));
            if (leafReload < 0) { throw new Exception(); }
        } catch (Exception e) {
            leafReload = 1;
            Log.warn("Invalid property value for 'hit_count_leaf', defaulting to 1", e);
        }

        if ((nodeReload + leafReload)<=0) {
            Log.error("Sum of hit_* variables must be positive !");
            System.exit(1);
        }

        scrollPages = "true".equalsIgnoreCase(config.getProperty("scroll_pages",
                    "false"));

        keepPace = "true".equalsIgnoreCase(config.getProperty("keep_pace",
                    "false"));

        allowChangeLanguage =
                "true".equalsIgnoreCase(config.getProperty("change_language",
                        "false"));
        allowSetLanguage =
                "true".equalsIgnoreCase(config.getProperty("set_language",
                        "false"));


        Log.debug("Limits green="+greenLimit+", red="+redLimit);

        String authMethod = config.getProperty("auth", "password");
        if ("password".equalsIgnoreCase(authMethod)) {
            LGen.authMethod = AUTH_PASS;
        } else if ("uniqueid".equalsIgnoreCase(authMethod)) {
            LGen.authMethod = AUTH_SUB;
        } else {
            throw new RuntimeException("Unknown authentication method : "+
                    authMethod);
        }

        useCache = "true".equalsIgnoreCase(config.getProperty("use_cache",
                    "true"));

        String sFileName = config.getProperty("subscriber_file");
        if (sFileName == null) {
            Log.warn("Using default subscriber source : none");
            sFileName = "none";
        }

        if ("none".equalsIgnoreCase(sFileName)) {
            subscriberSource = SUBSRC_DB;
        } else {
            subscriberSource = SUBSRC_FILE;
            File sFile = new File(sFileName);
            if (!sFile.exists()) {
                throw new RuntimeException("Specified subscriber list file "+
                        sFile.getAbsolutePath()+" doesn't exist");
            }

            FileReader fr = new FileReader(sFile);
            BufferedReader br = new BufferedReader(fr);
            subscribers = new Vector();
            while (true) {

                String line = br.readLine();
                if (line == null) { break; }

                String userName;
                String password = LGen.password;

                int colonIdx = line.indexOf(':');

                if (colonIdx > 0) {
                    userName = line.substring(0, colonIdx).trim();
                    password = line.substring(colonIdx+1).trim();
                } else {
                    userName = line.trim();
                }

                if ("".equals(userName)) { continue; }

                SubInfo si = new SubInfo(userName, password);
                subscribers.add(si);
            }

            Log.info("Loaded "+subscribers.size()+" subscribers from "+
                    sFile.getAbsolutePath());

            if (subscribers.size() == 0) {
                throw new RuntimeException("Require at least one subscriber !");
            }
        }

        if (useCache) {

            String defaultCacheDir;
            if (File.separatorChar == '/') {
                defaultCacheDir = "/tmp";
            } else {
                defaultCacheDir = "c:\\";
            }

            String cacheDir = config.getProperty("cache_dir", defaultCacheDir);
            File test = new File(cacheDir);

            if (!test.exists()) {
                test.mkdirs();
            }

            if (!test.exists() || !test.isDirectory() ||
                    !test.canRead() || !test.canWrite()) {
                Log.error("File '"+test.getAbsolutePath()+
                        "' is not a directory, doesn't exist or can not be accessed.\nPlease modify 'cache_dir' property accordingly. Cacheing is turned off");
                useCache = false;
            } else {
                String fileName = url.replace('/', '_').replace(':','^')+".lgc";
                cacheFileName = (new File(test, fileName)).getAbsolutePath();
                Log.info("Cache file to use : "+cacheFileName);
            }
        }
    }

    static void startLoad() throws Exception {
        stmt = dbConn.createStatement();

        loadSubById = dbConn.prepareStatement(LOAD_SUB_ID_QUERY);
        loadSubByLogin = dbConn.prepareStatement(LOAD_SUB_LOGIN_QUERY);

        switch (LGen.authMethod) {
        case AUTH_PASS:
            Log.info("Using password authentication");
            password = config.getProperty("password", "");
            if ("".equals(password)) {
                Log.warn("'password' property is empty or not set");
            }
            break;
        case AUTH_SUB:
            Log.info("Using unique id authentication");
            uniqueHeader = config.getProperty("unique-id", "x-up-subno");
            Log.info("Using header '"+uniqueHeader+"' for auth");
            break;
        default:
            throw new RuntimeException();
        }

        window = new Win();
        new WA();
        new MA();

        window.setLayout(null);

        window.setVisible(true);

        window.toFront();
        window.setSize(window.getPreferredSize());

        runTime = System.currentTimeMillis();

        for (int i=0; i<active_threads; i++) {
            (new Exec(i)).start();
        }

        (new Thread(window)).start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                setName("LG Shutdown");
                allStop = true;
                Log.info("Shutting down...");
                try {
                    if (allSubRS != null) {
                        allSubRS.close();
                    }
                    if (stmt != null) {
                        stmt.close();
                    }
                    if (dbConn != null) {
                        dbConn.close();
                    }
                } catch (Throwable e) {
                    Log.error("Error when cleaning up...", e);
                }
            }
        });
    }

    static long getRandom(SubInfo si, int mode) throws Exception {

        Vector which;

        switch (mode) {
            case CAT_NODE:
                which = si.nodeCategories;
                break;
            case CAT_LEAF:
                which = si.leafCategories;
                break;
            default:
                throw new RuntimeException("Unknown mode specified "+mode);
        }

        int idx = random.nextInt(which.size());
        return ((CategoryInfo)which.elementAt(idx)).categoryId;
    }

    static class MA extends MouseAdapter {
        MA() {
            window.addMouseListener(this);
        }

        public void mousePressed(MouseEvent e) {
            if ((!onWindows && e.isPopupTrigger()) ||
                    (onWindows && (e.getButton() == MouseEvent.BUTTON2 ||
                    e.getButton() == MouseEvent.BUTTON3))) {
                window.popupMenu(e.getX(), e.getY());
            }
        }
    }

    static class WA extends WindowAdapter {

        WA() {
            window.addWindowListener(this);
        }

        public void windowClosing(WindowEvent et) {
            Log.info("UI window is being closed, shutting down...");
            System.exit(0);
        }
    }

    static class TimeStat {
        long rounds;
        int latestTime;
        long totalTime;
        TimeStat(){}
        TimeStat(TimeStat ts) {
            rounds = ts.rounds;
            latestTime = ts.latestTime;
            totalTime = ts.totalTime;
        }
        void reset() {
            rounds = totalTime = 0L;
            latestTime = 0;
        }
    }

    private static Color getDistinctColor(int time) {
        // let's decide on the color.

        float femida = 0;   // 0 - good.
                            // 1 -- bad.

        if (time > greenLimit) {

            if (time > redLimit) {
                // just can't be any worse than that
                femida = 1.0F;
            } else {
                femida = ((float)time - (float)greenLimit) /
                    (float)(redLimit - greenLimit);
            }
        }

        return new Color(femida, (1.0F-femida)*0.7F, 0);
    }


    static class Win extends Frame implements Runnable, ActionListener {

        Graphics offScreen;
        Image iscreen;
        int scaleLt = -1;
        int ow,oh;
        int ol,ot;

        int barsPerLine;
        int lines;

        int rqCount;
        int lgCount;
        int ecCount;

        int moreCount;

        int topHeight;
        long totalTime;

        long redCount;
        long greenCount;

        int screenW;

        Color bg;
        Color fg;

        FontMetrics fm;
        Font font;

        Map buckets = new HashMap();
        final Object bucketLock = new Object();

        // boolean changed = true;
        boolean [] changed;

        int [] times = new int[max_threads];
        boolean [] login = new boolean[max_threads];
        long [] errorStamp = new long[max_threads];
        boolean [] alive = new boolean[max_threads];

        PopupMenu pm;

        Win() {
            super(APP);
            if (maxWidth > 0) {
                screenW = maxWidth;
            } else {
                screenW = Toolkit.getDefaultToolkit().getScreenSize().width;
            }
            changed = new boolean[max_threads];
            for (int i=0; i<max_threads;i++) {
                changed[i] = true;
            }
            setResizable(false);

            MenuItem mi;

            pm = new PopupMenu(APP);

            mi = new MenuItem("Throttle...");
            mi.setActionCommand(AC_THROTTLE);
            mi.addActionListener(this);
            pm.add(mi);

            mi = new MenuItem("Page statistics...");
            mi.setActionCommand(AC_PAGE_STAT);
            mi.addActionListener(this);
            pm.add(mi);

            mi = new MenuItem("Reset");
            mi.setActionCommand(AC_RST);
            mi.addActionListener(this);
            pm.add(mi);

            mi = new MenuItem("Quit");
            mi.setActionCommand(AC_QUIT);
            mi.addActionListener(this);
            pm.add(mi);

            add(pm);
        }

        void popupMenu(int x, int y) {
            pm.show(this, x, y);
        }

        public void actionPerformed(ActionEvent e) {
            String cmd = e.getActionCommand();

            if (AC_RST.equals(cmd)) {

                Log.info("Resetting statistics...");
                rqCount = 0;
                lgCount = 0;
                ecCount = 0;
                totalTime = 0;
                redCount = 0;
                greenCount = 0;
                moreCount = 0;

                synchronized (bucketLock) {
                    for (Iterator i = buckets.values().iterator();
                            i.hasNext(); ) {
                        TimeStat ts = (TimeStat)i.next();
                        ts.reset();
                    }
                }

                paintSync(true);

            } else if (AC_QUIT.equals(cmd)) {
                Log.info("User requested termination.");
                System.exit(0);
            } else if (AC_THROTTLE.equals(cmd)) {
                displayThrottleWin();
            } else if (AC_PAGE_STAT.equals(cmd)) {
                displayPageStatWin();
            } else if (AC_CLOSE_TW.equals(cmd)) {
                throttleWindow.dispose();
                throttleWindow = null;
            } else if (AC_APPLY_TV.equals(cmd)) {
                ThrottleWin w = throttleWindow;
                if (w != null) {
                    int at = w.canvas.current_value;
                    if (at < 1) { at = 1; }
                    if (at > max_threads) { at = max_threads; }
                    active_threads = w.canvas.current_value = at;
                    w.canvas.drawMe();
                }
            } else if (AC_RESET_TV.equals(cmd)) {
                ThrottleWin w = throttleWindow;
                if (w != null) {
                    w.canvas.current_value = active_threads;
                    w.canvas.drawMe();
                }
            } else if (AC_CLOSE_PSW.equals(cmd)) {
                PageStatWin w = pageStatWindow;
                pageStatWindow = null;
                if (w != null) {
                    w.dispose();
                }
            } else {
                Log.warn("Unknown action cmd : '"+cmd+'\'');
            }
        }

        public void run() {

            setName("AWT sync");

            try {

                while (true) {
                    if (allStop) { return; }
                    try {
                        Thread.sleep(awtPeriod);
                    } catch (Exception e) {
                    }
                    paintSync(false);
                    PageStatWin w = pageStatWindow;
                    if (w != null && w.canvas != null) {
                        w.canvas.drawMe();
                        w.canvas.repaint();
                    }
                }
            } catch (Throwable e) {
                Log.error("FATAL : AWT update thread caught an exception", e);
                System.exit(1);
            }
        }

        public Dimension getMinimumSize() {

            Insets insets = getInsets();
            int vDrag = insets.top + insets.bottom;
            int hDrag = insets.left + insets.right;

            int barsPerLine = (screenW - hDrag - scaleWidth) /
                (barWidth + barGap);

            if (fm == null) {
                font = getFont();
                if (font!=null) {
                    fm = getFontMetrics(font);
                }
            }

            if (fm != null) {
                topHeight = 2 * (fm.getMaxAscent() + fm.getMaxDescent());
            }

            vDrag += topHeight;

            Log.debug("Can fit max "+barsPerLine+" bars per line");

            if (max_threads > barsPerLine) {

                int lines = max_threads / barsPerLine;
                if (max_threads % barsPerLine > 0) {
                    lines++;
                }

                barsPerLine = max_threads / lines;
                if (max_threads % lines > 0) {
                    barsPerLine++;
                }

                this.barsPerLine = barsPerLine;
                this.lines = lines;

                Log.debug("size: "+lines+" lines"+", "+
                        barsPerLine+" bars per line");

                return new Dimension(barsPerLine * barWidth +
                        (barsPerLine-1)*barGap + scaleWidth + hDrag,
                        singleHeight * lines + vDrag);
            }

            this.barsPerLine = barsPerLine;
            lines = 1;

            return new Dimension(max_threads * barWidth +
                    (max_threads-1)*barGap + scaleWidth + hDrag,
                    singleHeight + vDrag);
        }

        public Dimension getMaximumSize() {
            return getMinimumSize();
        }

        public Dimension getPreferredSize() {
            return getMinimumSize();
        }

        void initGr() {

            Dimension size = getPreferredSize();
            setSize(size);
            Insets split = getInsets();

            int w = size.width - (split.left + split.right);
            int h = size.height - (split.top + split.bottom);

            Image im = createImage(w, h);
            if (im == null) { return; }

            offScreen = im.getGraphics();
            iscreen = im;
            ow = w;
            oh = h;

            ol = split.left;
            ot = split.top;

            font = getFont();
            fm = getFontMetrics(font);

            bg = getBackground();
            fg = getForeground();
        }

        void countMore() {
            moreCount++;
        }

        private void reportBucket(int bucket, int delta) {

            Integer bucketKey = new Integer(bucket);
            TimeStat ts = (TimeStat)buckets.get(bucketKey);
            if (ts == null) {
                synchronized (bucketLock) {
                    ts = (TimeStat)buckets.get(bucketKey);
                    if (ts == null) {
                        ts = new TimeStat();
                        buckets.put(bucketKey, ts);
                    }
                }
            }
            ts.rounds ++;
            ts.totalTime += delta;
            ts.latestTime = delta;
        }

        void report(int bucket, int thread, int delta) {

            synchronized (paintLock) {

                rqCount++;
                totalTime += (long)delta;
                if (times[thread] != delta) {
                    changed[thread] = true;
                    times[thread] = delta;
                }
                if (delta > redLimit) {
                    redCount++;
                } else {
                    greenCount++;
                }
                reportBucket(bucket, delta);
            }
        }

        void reportAlive(int thread) {
            synchronized (paintLock) {
                changed[thread] = alive[thread] = true;
                errorStamp[thread] = 0L;
                times[thread] = 0;
                login[thread] = false;
            }
        }

        void reportDead(int thread) {
            synchronized (paintLock) {
                changed[thread] = true;
                alive[thread] = false;
                times[thread] = 0;
            }
        }

        void reportError(int thread) {
            synchronized (paintLock) {
                ecCount++;
                changed[thread] = true;
                errorStamp[thread] = System.currentTimeMillis();
            }
        }

        void countLogin() {
            lgCount++;
        }

        void reportLogin(int thread, boolean way) {
            synchronized (paintLock) {
                if (login[thread] != way) {
                    login[thread] = way;
                    changed[thread] = true;
                }
            }
        }

        void paintSync(boolean force) {

            if (offScreen == null) {
                initGr();
            }

            if (offScreen == null) {
                // ok, we'll wait until the next time to update.
                return;
            }

            // draw statistics.
            drawStats();

            boolean changed[] = new boolean[max_threads];

            // let's retrieve current state of 'changed', and replace
            // them fast enough, but syncrhonously. So we don't loose
            // any pending changes while drawing.
            synchronized (paintLock) {
                boolean oneChanged = false;
                for (int i=0; i<max_threads; i++) {
                    if (oneChanged = this.changed[i]) {
                        break;
                    }
                }

                if (!oneChanged) {
                    // repaint();
                    if (!force) { return; }
                }

                System.arraycopy(this.changed, 0, changed, 0, max_threads);
                for (int i=0; i<max_threads; i++) {
                    this.changed[i] = false;
                }
            }

            // ok, we can draw now.

            // let's determine the scale

            int maxrt = times[0];

            for (int i=1; i<max_threads; i++) {
                maxrt = Math.max(times[i], maxrt);
            }

            // don't go over scaleBuffer
            maxrt = (int)((double)maxrt / scaleBuffer);

            // say minimum scale is minScale
            maxrt = Math.max(minScale, maxrt);

            // we don't want scale to be redrawn too often.
            // Let's allow current maximum to be within scaleTolerance of the
            // scale.

            boolean scaleChanged = (maxrt > scaleLt) ||
                    (maxrt < (int)((double)scaleLt * scaleTolerance));

            if (scaleChanged) {
                scaleLt = maxrt;
                drawScale();
            }

            long now = System.currentTimeMillis();

            for (int i=0; i<max_threads; i++) {
                if (scaleChanged || changed[i]) {
                    drawBar(i, now);
                }
            }

            repaint();
        }

        private void drawStats() {

            offScreen.setColor(bg);
            offScreen.fillRect(0, 0, ow, topHeight);
            // offScreen.setColor(fg);

            String s1 = "r : "+rqCount;
            String s2 = "e : "+ecCount;
            String s3 = "l : "+lgCount;

            int average = 0;
            try {
                average = (int)(totalTime / rqCount);
            } catch (ArithmeticException e) {}

            String s4 = "avg : "+average;

            int ws1 = fm.stringWidth(s1);
            int ws2 = fm.stringWidth(s2);
            int ws3 = fm.stringWidth(s3);
            // int ws4 = fm.stringWidth(s4);

            int max_len = ow / 4;

            // TODO here : need to be able to shift text LEFT,
            // in case there is not enough space.
            //
            // int s1_over = Math.max(0, ws1-max_len);
            // int s2_over = Math.max(0, ws2-max_len);
            // int s3_over = Math.max(0, ws3-max_len);
            // int s4_over = Math.max(0, ws4-max_len);

            int x1 = 0;
            int x2 = Math.max(x1+ws1, max_len);
            int x3 = Math.max(x2+ws2, max_len*2);
            int x4 = Math.max(x3+ws3, max_len*3);

            int y = fm.getMaxAscent();

            offScreen.setColor(green);
            offScreen.drawString(s1, x1, y);
            offScreen.setColor(Color.RED);
            offScreen.drawString(s2, x2, y);
            offScreen.setColor(fg);
            offScreen.drawString(s3, x3, y);
            offScreen.setColor(getDistinctColor(average));
            offScreen.drawString(s4, x4, y);

            y += fm.getMaxAscent() + fm.getMaxDescent();
            s1 = "time : "+ getNiceTime(System.currentTimeMillis() - runTime);

            if ((redCount + greenCount) > 0) {
                s2 = "r% : "+
                    getNiceDouble((double)redCount /
                            ((double)redCount + (double)greenCount) * 100.0D);
            } else {
                s2 = "r% : 0";
            }

            if ((rqCount + ecCount + lgCount) > 0) {
                s3 = "e% : "+
                    getNiceDouble(100.0D * (double)ecCount /
                            ((double)ecCount + (double)lgCount +
                             (double)rqCount));
            } else {
                s3 = "e% : 0";
            }

            if (rqCount > 0) {
                s4 = "m : "+moreCount + '(' + getNiceDouble(100.0D *
                        (double)moreCount / (double)rqCount) + "%)";
            } else {
                s4 = "m : 0";
            }

            ws1 = fm.stringWidth(s1);
            ws2 = fm.stringWidth(s2);
            ws3 = fm.stringWidth(s3);

            max_len = ow / 4;
            x1 = 0;
            x2 = Math.max(x1+ws1, max_len);
            x3 = Math.max(x2+ws2, max_len*2);
            x4 = Math.max(x3+ws3, max_len*3);

            offScreen.setColor(fg);
            offScreen.drawString(s1, x1, y);
            offScreen.setColor(Color.RED);
            offScreen.drawString(s2, x2, y);
            // offScreen.setColor(Color.RED);
            offScreen.drawString(s3, x3, y);
            offScreen.setColor(Color.BLACK);
            offScreen.drawString(s4, x4, y);
        }

        private String getNiceDouble(double val) {
            val *= 100;
            long x = Math.round(val);
            return ""+(x/100)+'.'+(x%100);
        }

        private String getNiceTime(long time) {

            time /= 1000;

            if (time == 0) {
                return "0 s";
            }

            String str = ""+(time % 60)+" s";
            time /= 60;
            if (time > 0) {
                str = ""+(time % 60)+" m "+str;
                time /= 60;
                if (time > 0) {
                    str = ""+(time % 24)+" h "+str;
                    time /= 24;
                    if (time > 0) {
                        str = ""+time+" d "+str;
                    }
                }
            }
            return str;
        }

        static Color green = new Color(0, 0.6F, 0);

        private void drawBar(int i, long now) {

            int bottom = fm.getMaxAscent() + fm.getMaxDescent();
            int barHeight = singleHeight - bottom;

            // rescale
            int time = (int)((double)times[i] *
                    (double)barHeight / (double)scaleLt);

            int x = scaleWidth + (i%barsPerLine) * (barWidth + barGap);
            int topY = topHeight + (i/barsPerLine)*singleHeight;

            offScreen.setColor(bg);
            offScreen.fillRect(x, topY, barWidth, singleHeight);

            offScreen.setColor(getDistinctColor(times[i]));
            offScreen.fillRect(x, topY + (barHeight-time), barWidth, time);

            if (!alive[i]) {

                offScreen.setColor(Color.darkGray);
                x += (barWidth - bottom) / 2;
                int y = topY + barHeight;
                offScreen.fillOval(x, y, bottom-2, bottom-2);

            } else if ((now - errorStamp[i])<errorLinger) {

                offScreen.setColor(Color.RED);
                x += (barWidth - bottom) / 2;
                int y = topY + barHeight;
                offScreen.fillOval(x, y, bottom-2, bottom-2);

            } else if (login[i]) {

                offScreen.setColor(fg);
                String s = String.valueOf("LOGIN");
                int tw = fm.stringWidth(s);
                x += (barWidth - tw) / 2;
                int y = topY + singleHeight - fm.getMaxDescent();
                offScreen.drawString("LOGIN", x, y);

            } else {

                offScreen.setColor(fg);
                String s = String.valueOf(times[i]);
                int tw = fm.stringWidth(s);
                x += (barWidth - tw) / 2;
                int y = topY + singleHeight - fm.getMaxDescent();
                offScreen.drawString(s, x, y);

            }
        }

        /*
        public void update(Graphics g) {
            // super.update(g);
            g.drawImage(iscreen, ol, ot, null);
        }
        */

        public void update(Graphics g) {
            paint(g);
        }

        public void paint(Graphics g) {
            if (iscreen == null) { return; }
            g.drawImage(iscreen, ol, ot, null);
        }

        private void drawScale() {

            // draw empty bar there.
            offScreen.setColor(bg);
            offScreen.fillRect(0, topHeight, ow, oh-topHeight);
            // offScreen.setColor(fg);
            offScreen.setColor(getDistinctColor(scaleLt));

            String s = String.valueOf(scaleLt);
            int tw = fm.stringWidth(s);
            int x = (scaleWidth - tw) / 2;
            int y = topHeight + fm.getMaxAscent();

            for (int i=0; i<lines; i++) {
                offScreen.drawString(s, x, i*singleHeight + y);
            }
        }
    }

    static class Exec extends Thread {

        int order;

        int doNodes;
        int doLeaves;

        IDocument nodePage;
        IDocument leafPage;

        long lastRequest;

        private Exec(int order) {
            this.order = order;
        }

        public void run() {

            setName("LD-"+order);

            try {
                synchronized (thread_monitor) {
                    if (thread_monitor[order]) {
                        Log.error("Thread "+order+
                                " seems to be already running !!!");
                        return;
                    }
                    thread_monitor[order] = true;
                }
                run0();
            } catch (StopE e) {
                return;
            } catch (Throwable e) {
                setName("LD-"+order+" (dying)");
                window.reportError(order);
                // e.printStackTrace();
                Log.error("thread failed", e);
                if (allStop) { return; }
                try {
                    Thread.sleep(sleepError);
                } catch (Exception e0) {}
                (new Exec(order)).start();
                Log.info("Thread "+order+" restarted.");
            } finally {
                window.reportDead(order);
                synchronized (thread_monitor) {
                    thread_monitor[order] = false;
                }
            }
        }

        private void run0() throws Exception {
            while (true) {
                if (allStop) { return; }
                doSingle();
            }
        }

        private void doSingle() throws Exception {

            window.reportAlive(order);

            checkStop();

            window.reportLogin(order, true);

            SubInfo si = getNextSub();

            leafPage = nodePage = null;
            doNodes = doLeaves = 0;

            Log.debug("Switching to subscriber "+si.login);

            /*
            RqRp rr = new RqRp(new URI(baseURL+
                        "subscriber/desktop_logout.do?logout=true"));
            rr.setRequestMethod(C.RM_GET);
            rr.engage();
            */

            CDS.cleanCookies();

            RqRp rr;
            IDocument d;
            Form lf;
            NodeList nl;

            if (authMethod == AUTH_PASS) {

                rr = new RqRp(new URI(baseURL+"subscriber/dv3.do"));
                rr.setRequestMethod(C.RM_GET);

                long before = System.currentTimeMillis();
                rr.engage();
                long so_far = System.currentTimeMillis() - before;

                d = (IDocument)rr.createHTMLDocument();

                so_far += loadImages(d);
                window.report(BKT_FRONT, order, (int)so_far);

                for (int i=0; i<2; i++) {

                    lf = null;
                    nl = d.getElementsByTagName("form");
                    if (nl.getLength() > 0) {
                        lf = new Form((Element)nl.item(0));
                    }

                    if (lf == null) {
                        throw new RuntimeException("No login form displayed for front page, while trying subscriber "+ si.login + sdoc(d));
                    }

                    lf.replaceText("username", si.login);
                    lf.replaceText("password", password);

                    rr = lf.prepareRequest();

                    before = System.currentTimeMillis();
                    rr.engage();
                    so_far = System.currentTimeMillis() - before;

                    d = (IDocument)rr.createHTMLDocument();

                    so_far += loadImages(d);

                    window.report(BKT_LOGIN, order, (int)so_far);

                    if (d.grepText("CDS Log In").getLength() == 0) {
                        break;
                    }

                    checkStop();

                    // try second time.
                }
            } else if (authMethod == AUTH_SUB) {

                rr = new RqRp(new URI(baseURL+"/subscriber/dv1.do"));
                rr.setRequestMethod(C.RM_GET);
                rr.getSendHeaders().setHeader(uniqueHeader, si.uid);
                long before = System.currentTimeMillis();
                rr.engage();
                long so_far = System.currentTimeMillis() - before;
                d = (IDocument)rr.createHTMLDocument();
                so_far += loadImages(d);
                window.report(BKT_FRONT, order, (int)so_far);

            } else {
                throw new RuntimeException("unknown auth method ??? ("+
                        authMethod+')');
            }

            if (d.grepText("Set Language").getLength() > 0) {
                // never logged in, need a language

                if (!allowSetLanguage) {
                    Log.info("Skipping subscriber "+si.login+
                            ", as he/she doesn't have a language set yet");
                    return;
                }

                Log.info("Selecting language for "+si.login);

                nl = d.getElementsByTagName("form");
                if (nl.getLength() > 0) {
                    lf = new Form((Element)nl.item(0));
                } else {
                    Log.error("Failed to find 'select language' form for "+
                            "subscriber "+ si.login + sdoc(d));
                    return;
                }

                lf.replaceText("l", "en_US");
                rr = lf.prepareRequest();
                rr.engage();

                // rr = new RqRp(new URI(baseURL + "subscriber/dv1.do"));
                // rr.setRequestMethod(C.RM_GET);
                // rr.engage();

                d = (IDocument)rr.createHTMLDocument();
            }

            if (d.grepText("Menu principal").getLength() > 0) {

                // this is french, let's reset the language.

                if (!allowChangeLanguage) {
                    Log.info("Skipping subscriber "+si.login+
                            ", as he/she speaks French");
                    return;
                }

                Log.info("Switching language to english for "+si.login);

                rr = new RqRp(new URI(baseURL + "subscriber/dv5.do"));
                rr.setRequestMethod(C.RM_GET);
                rr.engage();
                d = (IDocument)rr.createHTMLDocument();

                nl = d.getElementsByTagName("form");
                if (nl.getLength() > 0) {
                    lf = new Form((Element)nl.item(0));
                } else {
                    Log.error("Failed to find form for switching language" +
                            ", subscriber "+si.login + sdoc(d));
                    return;
                }

                lf.replaceText("l", "en_US");
                rr = lf.prepareRequest();
                rr.engage();

                rr = new RqRp(new URI(baseURL + "subscriber/dv1.do"));
                rr.setRequestMethod(C.RM_GET);
                rr.engage();

                d = (IDocument)rr.createHTMLDocument();
            }

            if (d.grepText("Main Menu").getLength() == 0) {
                window.reportError(order);
                Log.error("Failed to see 'Main Menu' string -- "+
                        "log in troubles ?, subscriber "+si.login + sdoc(d));
                return;
            }

            // we're logged in (well, hopefully)

            window.reportLogin(order, false);
            window.countLogin();

            for (int i=0; i<requestsPerUser; i++) {

                if (allStop) { return; }

                try {
                    request(si);
                } catch (StopE se) {
                    if (se.individual) {
                        Log.info("Thread "+getName()+" stopped.");
                    } else {
                        Log.debug("Thread stopped by all-stop", se);
                    }
                    throw se;
                }
            }
        }

        private void checkStop() throws Exception {
            if (allStop) {
                throw new StopE(false);
            }
            if (order >= active_threads) {
                throw new StopE(true);
            }
        }

        // this method needs to be called BEFORE the page processing.
        private void newPage() throws Exception {

            checkStop();

            // lastRequest is the time measured before the request
            // was prepared for processing
            if (lastRequest == 0) {
                lastRequest = System.currentTimeMillis();
            }
            long timeNow = System.currentTimeMillis();
            long sleepTime = threadSleep;
            if (keepPace) {
                sleepTime = Math.max((long)minThreadSleep,
                        sleepTime - (timeNow - lastRequest));
            }
            if (sleepTime > 0) {
                Thread.sleep(sleepTime);
            }
            lastRequest = System.currentTimeMillis();
        }

        private void request(SubInfo si) throws Exception {

            if (doNodes == 0 && doLeaves == 0) {
                doLeaves = leafReload;
                doNodes = nodeReload;
            }

            if (doNodes > 0) {
                doNodes--;
                nodePage = doNodeRequest(nodePage, si);
            } else if (doLeaves > 0) {
                doLeaves--;
                leafPage = doLeafRequest(leafPage, si);
            } else {
                Log.error("internal error -- nothing to do in a request ?");
            }
        }

        private RqRp findMore(IDocument page, int type) {

            if (!scrollPages) {
                return null;
            }

            String urlPath;
            switch (type) {
                case CAT_NODE:
                    urlPath = "dv7.do";
                    break;
                case CAT_LEAF:
                    urlPath = "dv38.do";
                    break;
                default:
                    throw new RuntimeException("unknown type "+type);
            }

            RqRp rr = null;

            if (page != null) {
                // look for "More"
                NodeList nl = page.grepText("More");
                for (int i=0; i<nl.getLength(); i++) {
                    Node parent = nl.item(i);

                    while (!(parent instanceof Document)) {

                        parent = parent.getParentNode();

                        if (parent instanceof HTMLAnchorElement) {

                            IAnchor ref = (IAnchor)parent;

                            if (ref.isLink() &&
                                    ref.getHref().indexOf(urlPath) > 0) {
                                rr = ref.prepareRequest();
                                if (rr == null) {
                                    Log.error("Failed to create request, href="+
                                            ref.getHref());
                                }
                                break;
                            }
                        }
                    }
                    if (rr != null) { break; }
                }

                if (rr != null) {
                    Log.debug("Following 'more' link from "+
                            page.getURI()+" to "+rr.getRequestURL());
                }
            }
            return rr;
        }

        private IDocument doNodeRequest(IDocument page,
                SubInfo si) throws Exception {

            newPage();

            RqRp rr = findMore(page, CAT_NODE);

            if (rr == null) {
                long catId = getRandom(si, CAT_NODE);
                rr = new RqRp(new URI(baseURL+"subscriber/dv7.do?catId="+
                            catId));
            } else {
                window.countMore();
            }

            long before = System.currentTimeMillis();

            rr.setRequestMethod(C.RM_GET);
            rr.engage();

            long so_far = System.currentTimeMillis() - before;

            Log.noise("Request returned in "+so_far+" msec");

            IDocument d = (IDocument)rr.createHTMLDocument();

            if (d.grepText("Select Category").getLength() == 0) {
                window.reportError(order);
                Log.error("No category list page returned. subscriber "+
                        si.login+", url="+rr.getRequestURL() + sdoc(d));
            } else if (d.grepText("no content is available").getLength() > 0) {
                window.reportError(order);
                Log.error("Empty category list returned. subscriber "+
                        si.login+", url="+rr.getRequestURL() + sdoc(d));
            }

            so_far += loadImages(d);

            window.report(BKT_CAT_LIST, order, (int)so_far);

            return d;
        }

        private long loadImages(IDocument page) {

            long totalTime = 0;

            NodeIterator ni =
                page.createNodeIterator(page.getDocumentElement(),
                         NodeFilter.SHOW_ELEMENT,
                         new NodeFilter(){

                         public short acceptNode(Node n) {
                            if ("img".equalsIgnoreCase(n.getNodeName())) {
                                return FILTER_ACCEPT;
                            }
                            return FILTER_SKIP;
                        }
                         
                         }, false);

            try {

                for (Node n = ni.nextNode(); n != null; n = ni.nextNode()) {

                    Element el = (Element)n;
                    String src = el.getAttribute("src");
                    if (src == null || "".equals(src)) {
                        Log.error("missing/empty src attribute for img tag "+
                                "on page "+page.getURI()+sdoc(page));
                        continue;
                    }
                    URI imgUri = page.getURI().resolve(src);
                    Log.noise("Loading image "+imgUri);
                    RqRp imgrr = new RqRp(imgUri, page.getURI().toString());
                    imgrr.setRequestMethod(C.RM_GET);
                    long before = System.currentTimeMillis();
                    imgrr.engage();
                    totalTime += System.currentTimeMillis() - before;
                    if (imgrr.getResponseCode() != 200) {
                        Log.error("bad response loading image from " + imgUri +
                                " server said : "+imgrr.getResponseCode() +
                                ' ' + imgrr.getResponseReason());
                    }
                }
            } catch (Exception e) {
                Log.error("Failed to load images for page "+page.getURI(), e);
            }

            return totalTime;
        }

        private IDocument doLeafRequest(IDocument page,
                SubInfo si) throws Exception {

            newPage();

            RqRp rr = findMore(page, CAT_LEAF);
            if (rr == null) {
                long catId = getRandom(si, CAT_LEAF);
                rr = new RqRp(new URI(baseURL+"subscriber/dv38.do?catId="+
                            catId));
            } else {
                window.countMore();
            }

            long before = System.currentTimeMillis();

            rr.setRequestMethod(C.RM_GET);
            rr.engage();

            long so_far = System.currentTimeMillis() - before;

            Log.noise("Request returned in "+so_far+" msec");

            IDocument d = (IDocument)rr.createHTMLDocument();

            if (d.grepText("Select Content").getLength() == 0) {
                window.reportError(order);
                Log.error("No content list page returned. subscriber "+
                        si.login+", url="+rr.getRequestURL() + sdoc(d));
                if (page != null) {
                    Log.debug("Previous page was:\n"+
                            W.prettyChar(page.getSource().toCharArray(), null));
                }
            } else if (d.grepText("no content is available").getLength() > 0) {
                window.reportError(order);
                Log.error("Empty content list returned. subscriber "+
                        si.login+", url="+rr.getRequestURL()+sdoc(d));
            }

            so_far += loadImages(d);
            window.report(BKT_CON_LIST, order, (int)so_far);

            if (chanceViewItem > 0.0F && (chanceViewItem == 1.0F ||
                        chanceViewItem > random.nextFloat())) {

                // pick item detail and execute so.
                NodeIterator ni =
                    ((IDocument)d).createNodeIterator(d.getDocumentElement(),
                             NodeFilter.SHOW_ELEMENT,
                             new NodeFilter(){

                             public short acceptNode(Node n) {
                                if ("a".equalsIgnoreCase(n.getNodeName())) {
                                    return FILTER_ACCEPT;
                                }
                                return FILTER_SKIP;
                            }
                             
                             }, false);

                Vector itemUrls = new Vector();

                for (Node n = ni.nextNode(); n != null; n = ni.nextNode()) {

                    Element el = (Element)n;
                    String href = el.getAttribute("href");
                    if (href == null ||
                            href.indexOf("/dv10.do")< 0) { continue; }
                    itemUrls.add(href);
                }

                for (Iterator i = itemUrls.iterator(); i.hasNext(); ) {
                    if (random.nextFloat() < chanceViewEveryItem) {
                        String url = (String)i.next();
                        loadItemDetails(si, d, url);
                    }
                }
            }
            return d;
        }

        private void loadItemDetails(SubInfo si, IDocument page,
                String href) throws Exception {

            newPage();

            URI dUri = page.getURI().resolve(href);
            RqRp rr = new RqRp(dUri, page.getURI().toString());
            rr.setRequestMethod(C.RM_GET);

            long before = System.currentTimeMillis();
            rr.engage();
            long so_far = System.currentTimeMillis() - before;

            Log.noise("Request returned in "+so_far+" msec");

            if (rr.getResponseCode() != 200) {
                throw new Exception("Unexpected server response : "+
                    rr.getResponseCode()+ rr.getResponseReason());
            }

            IDocument d = (IDocument)rr.createHTMLDocument();
            so_far += loadImages(d);
            window.report(BKT_DETAILS, order, (int)so_far);

            if (chancePurchaseItem > 0.0F &&
                    chancePurchaseItem > random.nextFloat()) {
                purchaseItem(si, d);
            }
        }

        private void purchaseItem(SubInfo si, IDocument page) throws Exception {

            int loopMax = 6;
            int loop = 0;
            long time_to_report = -1L;

            while (true) {

                if (loop >= loopMax) {
                    throw new Exception("Too many pages before download. "+
                            "sub="+si.login+
                            ", Current page="+page.getURI()+sdoc(page));
                }
                loop++;

                NodeIterator ni =
                    ((IDocument)page).createNodeIterator(page.getDocumentElement(),
                             NodeFilter.SHOW_ELEMENT,
                             new NodeFilter(){

                             public short acceptNode(Node n) {
                                if ("a".equalsIgnoreCase(n.getNodeName())) {
                                    return FILTER_ACCEPT;
                                }
                                return FILTER_SKIP;
                            }
                             
                             }, false);

                boolean latched = false;
                boolean downloaded = false;

                for (Node n = ni.nextNode(); n != null; n = ni.nextNode()) {

                    Element el = (Element)n;
                    String href = el.getAttribute("href");
                    if (href == null) { continue; }
                    if (href.indexOf("/dv12.do") > 0) {
                        // purchase URL

                        // that also means that the previous page we
                        // did (if any) was confirmation

                        if (time_to_report >= 0L) {
                            window.report(BKT_PUR_CF, order,
                                    (int)time_to_report);
                            time_to_report = -1L;
                        }
                        newPage();

                        URI next = page.getURI().resolve(href);
                        RqRp rr = new RqRp(next, page.getURI().toString());
                        rr.setRequestMethod(C.RM_GET);
                        long before = System.currentTimeMillis();
                        rr.engage();
                        if (rr.getResponseCode() != 200) {
                            throw new Exception("unexpected server response "+
                                    "for page "+next+
                                    " server said " + rr.getResponseCode() +
                                    ' ' + rr.getResponseReason()+", sub="+
                                    si.login);
                        }

                        long so_far = System.currentTimeMillis() - before;
                        page = (IDocument)rr.createHTMLDocument();
                        so_far += loadImages(page);
                        // TODO : separate confirmation from purchase !
                        // window.report(BKT_PUR, order, (int)so_far);
                        time_to_report = so_far;
                        latched = true;
                        break;
                    } else {

                        if (time_to_report >= 0L) {
                            window.report(BKT_PUR, order, (int)time_to_report);
                            time_to_report = -1L;   // reduntant
                        }

                        NodeList nl = page.grepText(el, "Download");
                        if (nl.getLength() > 0) {
                            // ok, consider this link to be a download link
                            downloadContent(si, page, href);
                            downloaded = true;
                            break;
                        }
                    }
                }

                if (latched) { continue; }
                if (downloaded) { break; }

                throw new Exception("No suitable links for purchase/download "+
                        "were found on page "+page.getURI()+", sub="+si.login+
                        sdoc(page));
            }
        }

        private void downloadContent(SubInfo si, IDocument page,
                String href) throws Exception {

            newPage();

            URI uri = page.getURI().resolve(href);
            RqRp rr = new RqRp(uri, page.getURI().toString());
            rr.setRequestMethod(C.RM_GET);
            rr.engage();
            if (rr.getResponseCode() != 200) {
                throw new Exception("unexpected server response for "+
                        uri+" from page "+page.getURI()+", server said : "+
                        rr.getResponseCode()+ ' ' + rr.getResponseReason() +
                        ", sub="+si.login);
            }

            String dlUrl = null;
            String cfUrl = null;

            String mimeType =
                rr.getResponseHeaders().getHeaderString(C.CONTENT_TYPE);

            if ("application/vnd.oma.dd+xml".equalsIgnoreCase(mimeType)) {

                IDocument d = (IDocument)rr.createHTMLDocument();

                if (d == null) {
                    throw new Exception("Returned OMA descriptor "+
                            "failed parsing, at "+uri+", sub="+si.login);
                }

                Node media = null;

                for (Node scan = d.getDocumentElement().getFirstChild();
                        scan != null; scan = scan.getNextSibling()) {
                    if ("media".equals(scan.getNodeName())) {
                        media = scan;
                        break;
                    }
                }

                if (media == null) {
                    throw new Exception("No \"media\" tag found in OMA "+
                            "descriptor from "+uri+", sub="+si.login+sdoc(d));
                }

                for (Node scan = media.getFirstChild(); scan != null;
                        scan = scan.getNextSibling()) {

                    String nn = scan.getNodeName();

                    if ("objectURI".equals(nn)) {
                        Node textNode = scan.getFirstChild();
                        if (textNode != null) {
                            String s = textNode.getNodeValue();
                            if (s != null) {
                                dlUrl = d.deEntity(s);
                            }
                        }
                    } else if ("installNotifyURI".equals(nn)) {
                        Node textNode = scan.getFirstChild();
                        if (textNode != null) {
                            String s = textNode.getNodeValue();
                            if (s != null) {
                                cfUrl = d.deEntity(s);
                            }
                        }
                    }
                }

            } else if ("text/x-pcs-gcd".equalsIgnoreCase(mimeType)) {

                byte [] _b = rr.getResponseData();
                ByteArrayInputStream bais = new ByteArrayInputStream(_b);
                InputStreamReader isr = new InputStreamReader(bais);
                BufferedReader br = new BufferedReader(isr);
                while (true) {
                    String s = br.readLine();
                    if (s == null) { break; }
                    s = s.toLowerCase();
                    if (s.startsWith("content-url:")) {
                        dlUrl = s.substring(s.indexOf(':')+1).trim();
                    }
                    if (s.startsWith("confirm-url:")) {
                        cfUrl = s.substring(s.indexOf(':')+1).trim();
                    }
                }

            } else if ("text/vnd.sun.j2me.app-descriptor".equalsIgnoreCase(mimeType)) {
                byte [] _b = rr.getResponseData();
                ByteArrayInputStream bais = new ByteArrayInputStream(_b);
                InputStreamReader isr = new InputStreamReader(bais);
                BufferedReader br = new BufferedReader(isr);
                while (true) {
                    String s = br.readLine();
                    if (s == null) { break; }
                    s = s.toLowerCase();
                    if (s.startsWith("midlet-jar-url:")) {
                        dlUrl = s.substring(s.indexOf(':')+1).trim();
                    }
                    if (s.startsWith("midlet-install-notify:")) {
                        cfUrl = s.substring(s.indexOf(':')+1).trim();
                    }
                }
            } else {
                Log.debug("Download is of unknown mime type "+mimeType+
                        ", assuming direct download");
                return;
            }

            if (dlUrl == null) {
                throw new Exception("No download url found in GCD, "+
                        "downloaded from "+href+" for sub="+si.login);
            }
            if (cfUrl == null) {
                Log.warn("No confirm URL found in GCD, downloaded from "+
                        href+" for sub="+si.login);
            }

            // TODO : verify the length and mime type

            int statusCode = 900; // success;

            try {
                rr = new RqRp(new URI(dlUrl));
                rr.setRequestMethod(C.RM_GET);
                rr.engage();
                if (rr.getResponseCode() != 200) {
                    throw new Exception("Unexpected response for download : "+
                            rr.getResponseCode() + " from "+dlUrl);
                }
            } catch (Exception e) {
                Log.warn("Error when downloading from "+dlUrl+" for sub="+
                        si.login, e);
                statusCode = 903;
            }

            if (cfUrl != null) {
                try {
                    rr = new RqRp(new URI(cfUrl));
                    rr.setRequestMethod(C.RM_POST);
                    byte [] b =
                        W.getMIDPStatusLine(statusCode).getBytes("UTF-8");
                    InputStream is = new ByteArrayInputStream(b);
                    rr.setData(b);
                    rr.engage();
                } catch (Exception e) {
                    Log.warn("Error when posting status "+statusCode+
                            " to "+cfUrl, e);
                }
            }
        }
    }

    static class SubInfo {
        String login;
        String password;
        String uid;
        long deviceId;
        long id;
        boolean loaded;// = false;
        private int srcType;

        Vector allowedCategories = new Vector();

        Vector leafCategories = new Vector();
        Vector nodeCategories = new Vector();

        /*
        SubInfo(String login, String uid, long deviceId, long id) {
            this.login = login;
            this.uid = uid;
            this.deviceId = deviceId;
            this.id = id;
            this.password = password;
        }
        */

        SubInfo(String login, String password) {
            this.login = login;
            this.password = password;
            srcType = SUBSRC_TYPE_LOGIN;
        }

        SubInfo(long id) {
            this.id = id;
            srcType = SUBSRC_TYPE_ID;
        }

        boolean load() throws Exception {
            if (loaded) { return true; }
            ResultSet rs = null;
            switch (srcType) {
                case SUBSRC_TYPE_ID:
                    loadSubById.setLong(1, id);
                    rs = loadSubById.executeQuery();
                    try {
                        if (!rs.next()) {
                            Log.warn("Failed to load subscriber for subscriber id "+id);
                            return false;
                        }
                        loadFromRS(rs);
                    } finally {
                        rs.close();
                    }
                    break;
                case SUBSRC_TYPE_LOGIN:
                    loadSubByLogin.setString(1, login);
                    rs = loadSubByLogin.executeQuery();
                    try {
                        if (!rs.next()) {
                            Log.warn("Failed to load subscriber for subscriber login "+login);
                            return false;
                        }
                        loadFromRS(rs);
                    } finally {
                        rs.close();
                    }
                    break;
                default:
                    throw new RuntimeException("Unknown subscriber source type : "+srcType);
            }
            return loaded;

        }

        void loadFromRS(ResultSet rs) throws Exception {
            login = rs.getString(1);
            uid = rs.getString(2);
            deviceId = rs.getLong(3);
            id = rs.getLong(4);

            if (password == null) {
                password = LGen.password;
            }
            loaded = true;
        }
    }

    static class DevInfo implements Serializable {
        private static final long serialVersionUID = 0xebabededaf0cL;
        CategoryInfo [] nodeCategories;
        CategoryInfo [] leafCategories;
    }

    static class CategoryInfo implements Serializable {
        private static final long serialVersionUID = 0xebabededaf1cL;

        long categoryId;
        int count;
        Vector children = new Vector();
        CategoryInfo parent;

        CategoryInfo(long id, int count) {
            this(id);
            this.count = count;
        }

        CategoryInfo(long id) {
            categoryId = id;
        }

        CategoryInfo(CategoryInfo ci) {
            categoryId = ci.categoryId;
            children = ci.children;
            parent = ci.parent;
        }

        public int hashCode() {
            return (int)categoryId;
        }

        public boolean equals(Object x) {
            if (!(x instanceof CategoryInfo)) {
                return false;
            }
            return categoryId == ((CategoryInfo)x).categoryId;
        }
    }

    static class StopE extends RuntimeException {

        boolean individual;

        StopE(boolean individual) {
            this.individual = individual;
        }
    }

    static class PageStatWin extends Frame {

        PSWCanvas canvas;

        PageStatWin() {
            super("Page Statistcs");
            setResizable(false);
            setLayout(new BorderLayout());
            addWindowListener(new WindowAdapter() {
                public void windowClosing(WindowEvent e) {
                    if (pageStatWindow != null) {
                        pageStatWindow.dispose();
                        pageStatWindow = null;
                    }
                }
            });
        }

        public void my_init() {
            add(canvas = new PSWCanvas(), BorderLayout.NORTH);
            Panel c = new Panel();

            c.setLayout(new FlowLayout());

            Button b;

            b = new Button("Close");
            b.setActionCommand(AC_CLOSE_PSW);
            b.addActionListener(window);
            c.add(b);

            setVisible(true);

            add(c, BorderLayout.SOUTH);
            setSize(getPreferredSize());
            c.doLayout();
            doLayout();
            Dimension ps = getPreferredSize();
            // Insets is = getInsets();
            // System.out.println(">>> pref size (PSW) : "+
            // ps.width+'x'+ps.height);
            // setSize(ps.width + is.left + is.right,
            // ps.height + is.top + is.bottom);
            setSize(ps);

            /*
            ps = getPreferredSize();
            System.out.println(">>> my pref size (PSW) : "+
            ps.width+'x'+ps.height);
            ps = c.getPreferredSize();
            System.out.println(">>> bb pref size (PSW) : "+
            ps.width+'x'+ps.height);
            ps = canvas.getPreferredSize();
            System.out.println(">>> cc pref size (PSW) : "+
            ps.width+'x'+ps.height);
            */

            setVisible(true);
            toFront();
            repaint();
        }

    }

    static class ThrottleWin extends Frame {

        Button apply;
        Button reset;
        Button close;
        ThrottleTP canvas;

        ThrottleWin() {
            super("Throttle Control");
            setResizable(false);
            setLayout(new BorderLayout());

            addWindowListener(new WindowAdapter() {
                public void windowClosing(WindowEvent e) {
                    if (throttleWindow != null) {
                        throttleWindow.dispose();
                        throttleWindow = null;
                    }
                }
            });
        }

        public void init() {
            add(new ThrottleBP(), BorderLayout.SOUTH);
            add(canvas = new ThrottleTP(), BorderLayout.NORTH);
            setSize(getPreferredSize());
            doLayout();
            setSize(getPreferredSize());
            setVisible(true);
            toFront();
            repaint();
        }
    }

    static class PSWCanvas extends Canvas {

        Image iscreen;
        Graphics offScreen;
        int ow, oh;
        int pw, ph;
        boolean textDone;
        Object drawLock = new Object();
        int textEnd;
        int lineHeight;
        int textHeight;

        PSWCanvas() {
            FontMetrics fm = window.getFontMetrics(window.getFont());
            int max = fm.stringWidth(bucketNames[0]);
            for (int i=1; i<bucketNames.length; i++) {
                int tw = fm.stringWidth(bucketNames[i]);
                if (tw > max) { max = tw; }
            }

            ph = ((textHeight = fm.getMaxAscent() + fm.getMaxDescent()) << 1) *
                bucketNames.length +
                (bucketNames.length - 1) * PSW_LINE_INTERVAL +
                PSW_TOP_INSET + PSW_BOTTOM_INSET;

            pw = (textEnd = max + PSW_LEFT_INSET + PSW_SPACE + textHeight) +
                PSW_WIDTH + PSW_RIGHT_INSET;

            lineHeight = textHeight << 1;
            lineHeight += PSW_LINE_INTERVAL;

            // System.out.println(">>> set preferred size : "+pw+'x'+ph);
        }

        public void update(Graphics g) {
            paint(g);
        }

        public void paint(Graphics g) {
            if (iscreen == null) {
                initGr();
                drawMe();
                repaint();
                return;
            }
            g.drawImage(iscreen, 0, 0, null);
        }

        public Dimension getMinimumSize() {
            return getPreferredSize();
        }

        public Dimension getMaximumSize() {
            return getPreferredSize();
        }

        public Dimension getPreferredSize() {
            return new Dimension(pw, ph);
        }

        void initGr() {
            Dimension size = getSize();
            // System.out.println("get size = "+size.width+'x'+size.height);
            if (size.width == 0 || size.height == 0) { return; }

            int w = size.width;
            int h = size.height;

            Image im = createImage(w, h);
            if (im == null) { return; }

            offScreen = im.getGraphics();
            iscreen = im;
            ow = w;
            oh = h;
        }

        void drawMe() {

            synchronized (drawLock) {

                if (pageStatWindow == null || offScreen == null) { return; }

                int fo = window.fm.getMaxAscent();

                Graphics g = offScreen;

                if (!textDone) {

                    g.setColor(Color.lightGray);
                    g.fillRect(0, 0, ow, oh);

                    int y = PSW_TOP_INSET;
                    int fmy = window.fm.getMaxAscent();

                    g.setColor(Color.BLACK);

                    for (int i=0; i<bucketNames.length; i++) {
                        g.drawString(bucketNames[i], PSW_LEFT_INSET,
                                y + fmy);

                        // draw runes

                        g.drawArc(textEnd - textHeight + 1, y + 1,
                                (int)(textHeight * 0.64), textHeight - 1,
                                90, -180);
                        g.drawArc(textEnd - (textHeight>>1) - 1, y + 1,
                                (int)(textHeight * 0.64), textHeight - 1,
                                -90, -180);

                        int quarter = (int)(textHeight * 0.25);

                        g.drawOval(
                                textEnd - 3 * quarter,
                                y + textHeight + quarter,
                                quarter << 1,
                                quarter << 1);
                        g.drawLine(
                                textEnd - textHeight + quarter,
                                y + (textHeight << 1) - quarter,
                                textEnd - quarter,
                                y + textHeight + quarter);

                        y += lineHeight;
                    }

                    textDone = true;
                }

                g.setColor(Color.lightGray);
                g.fillRect(textEnd, 0, PSW_WIDTH, oh);

                TimeStat [] vals = new TimeStat[bucketNames.length];
                long maxw = 0;
                for (int i=0; i<vals.length; i++) {
                    TimeStat copy =
                        (TimeStat)window.buckets.get(new Integer(i));
                    if (copy == null) { continue; }
                    vals[i] = new TimeStat(copy);

                    vals[i].totalTime /= vals[i].rounds;

                    if (maxw < vals[i].totalTime) { maxw = vals[i].totalTime; }

                    if (maxw < vals[i].latestTime) {
                        maxw = vals[i].latestTime;
                    }
                }

                if (maxw == 0) { return; }

                int y = PSW_TOP_INSET;
                double scale = (double)PSW_WIDTH / (double)maxw;
                int fmy = window.fm.getMaxAscent();

                for (int i=0; i<vals.length; i++) {

                    if (vals[i] == null) {
                        y += lineHeight;
                        continue;
                    }

                    TimeStat ts = (TimeStat)window.buckets.get(new Integer(i));
                    g.setColor(getDistinctColor(vals[i].latestTime));
                    g.fillRect(textEnd, y,
                        (int)((double)vals[i].latestTime * scale), textHeight);
                    g.setColor(getDistinctColor((int)vals[i].totalTime));
                    g.fillRect(textEnd, y + textHeight, 
                        (int)((double)vals[i].totalTime * scale), textHeight);

                    g.setColor(Color.BLACK);
                    g.drawString(String.valueOf(vals[i].latestTime),
                            textEnd + 1, y + fmy);
                    g.drawString(String.valueOf(vals[i].totalTime),
                            textEnd + 1, y + fmy + textHeight);

                    y+=lineHeight;
                }
            }
        }
    }

    static class ThrottleTP extends Canvas implements
            MouseMotionListener, MouseWheelListener, MouseListener,
            KeyListener {

        Image iscreen;
        Graphics offScreen;
        int ow, oh;

        Object drawLock = new Object();

        boolean mouseTrack = false;
        int trackPoint = -1;
        int track_value;

        int drawn_bar_loc = -1;
        int drawn_value = -1;

        int [] tiglX = new int[]{20, 380, 380};
        int [] tiglY = new int[]{70, 70, 40};

        int current_value = active_threads;
        int bar_loc = -1;
        double scale = 360.0D / (double)(max_threads - 1);

        ThrottleTP() {
            addMouseListener(this);
            addMouseWheelListener(this);
            addMouseMotionListener(this);
            addKeyListener(this);
        }

        void initGr() {
            Dimension size = getSize();
            if (size.width == 0 || size.height == 0) { return; }

            int w = size.width;
            int h = size.height;

            Image im = createImage(w, h);
            if (im == null) { return; }

            offScreen = im.getGraphics();
            iscreen = im;
            ow = w;
            oh = h;
        }

        void drawMe() {

            synchronized (drawLock) {

                if (throttleWindow == null || offScreen == null) { return; }

                if (current_value < 1) { current_value = 1; }
                if (current_value > max_threads) {
                    current_value = max_threads;
                }

                int fo = window.fm.getMaxAscent();

                Graphics g = offScreen;

                g.setColor(Color.lightGray);
                g.fillRect(0, 0, ow, oh);
                g.setColor(Color.gray);
                g.fillPolygon(tiglX, tiglY, 3);
                g.drawLine(20, 70, 380, 70);
                g.setColor(Color.black);
                g.drawString("1", 20, 70 + fo);
                g.drawString(String.valueOf(max_threads), 370, 70 + fo);

                bar_loc = (int)(scale * (double)(current_value-1)) + 20;
                g.setColor(Color.darkGray);
                g.fillRect(bar_loc - 7, 35, 15, 40);
                g.setColor(Color.black);
                g.drawLine(bar_loc, 35, bar_loc, 74);

                String val = String.valueOf(current_value);
                int tw = window.fm.stringWidth(val);
                g.drawString(val, bar_loc - (tw/2),
                        33 - window.fm.getMaxDescent());

                if (current_value == active_threads) {
                    throttleWindow.reset.setEnabled(false);
                    throttleWindow.apply.setEnabled(false);
                } else {
                    throttleWindow.reset.setEnabled(true);
                    throttleWindow.apply.setEnabled(true);
                }

                if (drawn_bar_loc != bar_loc ||
                        drawn_value != current_value) {
                    throttleWindow.repaint();
                    repaint();
                }

                drawn_bar_loc = bar_loc;
                drawn_value = current_value;
            }
        }

        public void update(Graphics g) {
            paint(g);
        }

        public void paint(Graphics g) {
            if (iscreen == null) {
                initGr();
                drawMe();
                repaint();
                return;
            }
            g.drawImage(iscreen, 0, 0, null);
        }

        public Dimension getMaximumSize() {
            return new Dimension(400,120);
        }

        public Dimension getMinimumSize() {
            return getMaximumSize();
        }

        public Dimension getPreferredSize() {
            return getMaximumSize();
        }

        public void mouseMoved(MouseEvent e) {}
        public void mouseClicked(MouseEvent e) {}
        public void keyReleased(KeyEvent e) {}
        public void keyTyped(KeyEvent e) {}
        public void mouseExited(MouseEvent e) {}

        public void mouseReleased(MouseEvent e) {
            mouseTrack = false;
        }

        public void mouseEntered(MouseEvent e) {
            requestFocus();
        }

        public void mousePressed(MouseEvent e) {
            /*
            System.out.println("&& got mev, x="+e.getX()+", y="+
                    e.getY()+", bar_loc="+bar_loc);
            */
            int x = e.getX();
            int y = e.getY();
            if (x > bar_loc - 7 && x < bar_loc + 7 &&
                    y > 25 && y < 75) {

                // System.out.println("&& tracking activated");

                mouseTrack = true;
                trackPoint = x;
                track_value = current_value;
            }
        }

        public void mouseDragged(MouseEvent e) {

            if (!mouseTrack) { return; }

            // for that change of 'X' , what would the new
            // value be ?
            current_value = track_value +
                (int)((double)(e.getX() - trackPoint) / scale);
            drawMe();
        }

        public void mouseWheelMoved(MouseWheelEvent e) {

            // we want "visible" scrolling !
            // value = x / scale
            // x += clicks => value += (clicks / scale)
            // value has to be integer =>
            // if scale > 1 => value += clicks, else value += clicks/scale

            if (scale >= 1) {
                current_value += e.getWheelRotation();
            } else {
                current_value += (int)((double)e.getWheelRotation() / scale);
            }
            drawMe();

        }

        public void keyPressed(KeyEvent e) {

            /*
            System.out.println("&& got key evt, char="+e.getKeyChar()+
                    ", code="+e.getKeyCode());
            */

            switch (e.getKeyCode()) {
                case KeyEvent.VK_KP_LEFT:
                case KeyEvent.VK_LEFT:
                case KeyEvent.VK_NUMPAD4:
                    current_value--;
                    drawMe();
                    break;
                case KeyEvent.VK_KP_RIGHT:
                case KeyEvent.VK_RIGHT:
                case KeyEvent.VK_NUMPAD6:
                    current_value++;
                    drawMe();
                    break;
            }
        }
    }

    static class ThrottleBP extends Panel {

        ThrottleBP() {

            setLayout(new FlowLayout());
            Button b;

            throttleWindow.apply = b = new Button("Apply");
            b.setActionCommand(AC_APPLY_TV);
            b.addActionListener(window);
            add(b);
            throttleWindow.reset = b = new Button("Reset");
            b.setActionCommand(AC_RESET_TV);
            b.addActionListener(window);
            add(b);
            throttleWindow.close = b = new Button("Close");
            b.setActionCommand(AC_CLOSE_TW);
            b.addActionListener(window);
            add(b);
        }

        public Dimension getMaximumSize() {
            return new Dimension(400,60);
        }

        public Dimension getMinimumSize() {
            return getMaximumSize();
        }

        public Dimension getPreferredSize() {
            return getMaximumSize();
        }

    }

    static String sdoc(IDocument d) {
        StringWriter sw = new StringWriter();
        try {
            d.writeDocument(sw, true, true);
        } catch (IOException e) {
            e.printStackTrace();
            return "<IO Exception>";
        }
        return "\n" + sw.toString();
    }

    static void displayThrottleWin() {

        if (throttleWindow != null) {
            throttleWindow.setExtendedState(Frame.NORMAL);
            throttleWindow.toFront();
        } else {
            throttleWindow = new ThrottleWin();
            throttleWindow.init();
        }
    }

    static void displayPageStatWin() {

        PageStatWin w = pageStatWindow;
        if (w != null) {
            pageStatWindow.setExtendedState(Frame.NORMAL);
            pageStatWindow.toFront();
        } else {
            w = pageStatWindow = new PageStatWin();
            w.my_init();
        }
    }
}