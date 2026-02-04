namespace DefectDataAudio
{
    public class ProductionData
    {
        public DateTime DateTime { get; set; } 
        public string  MachineCode { get; set; }      
        public string Model { get; set; }  
        public string Defect { get; set; }    
        public string Reason_Defect { get; set; } 
        public string Station { get; set; }
        public int Quantity { get; set; }
        
    }

    public class LineConfig
    {
        public string Name { get; set; }
        public string Ip { get; set; }
        public string TableName { get; set; }
    }
}