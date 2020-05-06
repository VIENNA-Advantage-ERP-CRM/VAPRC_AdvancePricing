using System.Web.Mvc;
using System.Web.Optimization;



//NOTE:--    Please replace ViennaAdvantage with Module Prefix Code(MPC)..



namespace VAPRC //  Please replace namespace with  Module Prefix Code(MPC)..
{
    public class ViennaAdvantageAreaRegistration : AreaRegistration
    {
        public override string AreaName
        {
            get
            {
                return "VAPRC";   //Please replace "ViennaAdvantage" with  Module Prefix Code(MPC)..
            }
        }

        public override void RegisterArea(AreaRegistrationContext context)
        {
            context.MapRoute(
                "VAPRC_default",
                "VAPRC/{controller}/{action}/{id}",
                new { action = "Index", id = UrlParameter.Optional }
                , new[] { "VAPRC.Controllers" }
            );    // Please replace ViennaAdvantage with Module Prefix Code(MPC)..


            StyleBundle style = new StyleBundle("~/Areas/VAPRC/Contents/VAPRCStyle");

            /* ==>  Here include all css files in style bundle......see example below....  */

            style.Include("~/Areas/VAPRC/Contents/VAPRC.css");
            //              "~/Areas/ViennaAdvantage/Contents/example2.css");





            ScriptBundle script = new ScriptBundle("~/Areas/VAPRC/Scripts/VAPRCJs");

            /*-------------------------------------------------------
                    Here include all js files in style bundle......see example below....
             --------------------------------------------------------*/


            script.Include("~/Areas/VAPRC/Scripts/model/callouts.js");
            //               "~/Areas/ViennaAdvantage/Scripts/example2.js");




            /*-------------------------------------------------------
              Please replace "ViennaAdvantage" with Module Prefix Code(MPC)..
             * 
             * 1. first parameter is script/style bundle...
             * 
             * 2. Second parameter is Module Prefix Code(MPC)..
             * 
             * 3. Third parameter is order of loading... (dafault is 10 )
             * 
             --------------------------------------------------------*/

            VAdvantage.ModuleBundles.RegisterScriptBundle(script, "VAPRC", 10);
            VAdvantage.ModuleBundles.RegisterStyleBundle(style, "VAPRC", 10);
        }
    }
}